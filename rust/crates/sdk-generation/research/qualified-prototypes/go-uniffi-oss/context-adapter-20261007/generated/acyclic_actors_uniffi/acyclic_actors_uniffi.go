
package acyclic_actors_uniffi

// #include <acyclic_actors_uniffi.h>
import "C"

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"
	"encoding/binary"
	"reflect"
	"context"
	"runtime/cgo"
	"math"
	"runtime"
	"sync/atomic"
)



// This is needed, because as of go 1.24
// type RustBuffer C.RustBuffer cannot have methods,
// RustBuffer is treated as non-local type
type GoRustBuffer struct {
	inner C.RustBuffer
}

type RustBufferI interface {
	AsReader() *bytes.Reader
	Free()
	ToGoBytes() []byte
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

// C.RustBuffer fields exposed as an interface so they can be accessed in different Go packages.
// See https://github.com/golang/go/issues/13467
type ExternalCRustBuffer interface {
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

func RustBufferFromC(b C.RustBuffer) ExternalCRustBuffer {
	return GoRustBuffer {
		inner: b,
	}
}

func CFromRustBuffer(b ExternalCRustBuffer) C.RustBuffer {
	return C.RustBuffer {
		capacity: C.uint64_t(b.Capacity()),
		len: C.uint64_t(b.Len()),
		data: (*C.uchar)(b.Data()),
	}
}

func RustBufferFromExternal(b ExternalCRustBuffer) GoRustBuffer {
	return GoRustBuffer {
		inner: C.RustBuffer {
			capacity: C.uint64_t(b.Capacity()),
			len: C.uint64_t(b.Len()),
			data: (*C.uchar)(b.Data()),
		},
	}
}

func (cb GoRustBuffer) Capacity() uint64 {
	return uint64(cb.inner.capacity)
}

func (cb GoRustBuffer) Len() uint64 {
	return uint64(cb.inner.len)
}

func (cb GoRustBuffer) Data() unsafe.Pointer {
	return unsafe.Pointer(cb.inner.data)
}

func (cb GoRustBuffer) AsReader() *bytes.Reader {
	b := unsafe.Slice((*byte)(cb.inner.data), C.uint64_t(cb.inner.len))
	return bytes.NewReader(b)
}

func (cb GoRustBuffer) Free() {
	rustCall(func( status *C.RustCallStatus) bool {
		C.ffi_acyclic_actors_uniffi_rustbuffer_free(cb.inner, status)
		return false
	})
}

func (cb GoRustBuffer) ToGoBytes() []byte {
	return C.GoBytes(unsafe.Pointer(cb.inner.data), C.int(cb.inner.len))
}


func stringToRustBuffer(str string) C.RustBuffer {
	return bytesToRustBuffer([]byte(str))
}

func bytesToRustBuffer(b []byte) C.RustBuffer {
	if len(b) == 0 {
		return C.RustBuffer{}
	}
	// We can pass the pointer along here, as it is pinned
	// for the duration of this call
	foreign := C.ForeignBytes {
		len: C.int(len(b)),
		data: (*C.uchar)(unsafe.Pointer(&b[0])),
	}
	
	return rustCall(func( status *C.RustCallStatus) C.RustBuffer {
		return C.ffi_acyclic_actors_uniffi_rustbuffer_from_bytes(foreign, status)
	})
}


type BufLifter[GoType any] interface {
	Lift(value RustBufferI) GoType
}

type BufLowerer[GoType any] interface {
	Lower(value GoType) C.RustBuffer
}

type BufReader[GoType any] interface {
	Read(reader io.Reader) GoType
}

type BufWriter[GoType any] interface {
	Write(writer io.Writer, value GoType)
}

func LowerIntoRustBuffer[GoType any](bufWriter BufWriter[GoType], value GoType) C.RustBuffer {
	// This might be not the most efficient way but it does not require knowing allocation size
	// beforehand
	var buffer bytes.Buffer
	bufWriter.Write(&buffer, value)

	bytes, err := io.ReadAll(&buffer)
	if err != nil {
		panic(fmt.Errorf("reading written data: %w", err))
	}
	return bytesToRustBuffer(bytes)
}

func LiftFromRustBuffer[GoType any](bufReader BufReader[GoType], rbuf RustBufferI) GoType {
	defer rbuf.Free()
	reader := rbuf.AsReader()
	item := bufReader.Read(reader)
	if reader.Len() > 0 {
		// TODO: Remove this
		leftover, _ := io.ReadAll(reader)
		panic(fmt.Errorf("Junk remaining in buffer after lifting: %s", string(leftover)))
	}
	return item
}



func rustCallWithError[E any, U any](converter BufReader[E], callback func(*C.RustCallStatus) U) (U, E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[E], status C.RustCallStatus) E {
	switch status.code {
	case 0:
		var zero E
		return zero
	case 1:
		return LiftFromRustBuffer(converter, GoRustBuffer { inner: status.errorBuf })
	case 3:
		return any(NewBindingErrorCancelled()).(E)
	case 2:
		// when the rust code sees a panic, it tries to construct a rustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer { inner: status.errorBuf })))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		panic(fmt.Errorf("unknown status code: %d", status.code))
	}
}

func checkCallStatusUnknown(status C.RustCallStatus) error {
	switch status.code {
	case 0:
		return nil
	case 1:
		panic(fmt.Errorf("function not returning an error returned an error"))
	case 3:
		return NewBindingErrorCancelled()
	case 2:
		// when the rust code sees a panic, it tries to construct a C.RustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer {
				inner: status.errorBuf,
			})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		return fmt.Errorf("unknown status code: %d", status.code)
	}
}

func rustCall[U any](callback func(*C.RustCallStatus) U) U {
	returnValue, err := rustCallWithError[error](nil, callback)
	if err != nil {
		panic(err)
	}
	return returnValue
}

type NativeError interface {
	AsError() error
}


func writeInt8(writer io.Writer, value int8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint8(writer io.Writer, value uint8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt16(writer io.Writer, value int16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint16(writer io.Writer, value uint16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt32(writer io.Writer, value int32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint32(writer io.Writer, value uint32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(writer io.Writer, value int64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint64(writer io.Writer, value uint64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat32(writer io.Writer, value float32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat64(writer io.Writer, value float64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}


func readInt8(reader io.Reader) int8 {
	var result int8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint8(reader io.Reader) uint8 {
	var result uint8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt16(reader io.Reader) int16 {
	var result int16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint16(reader io.Reader) uint16 {
	var result uint16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt32(reader io.Reader) int32 {
	var result int32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint32(reader io.Reader) uint32 {
	var result uint32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt64(reader io.Reader) int64 {
	var result int64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint64(reader io.Reader) uint64 {
	var result uint64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat32(reader io.Reader) float32 {
	var result float32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat64(reader io.Reader) float64 {
	var result float64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func init() {
        
        uniffiCheckChecksums()
}


func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 30
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_acyclic_actors_uniffi_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: UniFFI contract version mismatch")
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_func_connect_actors()
	})
	if checksum != 30034 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_func_connect_actors: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_func_connect_actors_with_ca()
	})
	if checksum != 36250 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_func_connect_actors_with_ca: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorid_value()
	})
	if checksum != 35989 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorid_value: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorlimits_checkpoint_bytes()
	})
	if checksum != 48311 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorlimits_checkpoint_bytes: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorlimits_handler_timeout_millis()
	})
	if checksum != 5285 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorlimits_handler_timeout_millis: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorlimits_memory_bytes()
	})
	if checksum != 11374 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorlimits_memory_bytes: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_add_subscription()
	})
	if checksum != 12476 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_add_subscription: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_checkpoint_actor()
	})
	if checksum != 46592 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_checkpoint_actor: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_create_actor()
	})
	if checksum != 50607 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_create_actor: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_inspect_actor()
	})
	if checksum != 159 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_inspect_actor: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_inspect_actor_request()
	})
	if checksum != 60417 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_inspect_actor_request: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_invoke_actor()
	})
	if checksum != 46887 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_invoke_actor: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_remove_subscription()
	})
	if checksum != 35201 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_remove_subscription: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_resume_subscription()
	})
	if checksum != 34597 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_resume_subscription: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_update_actor()
	})
	if checksum != 47360 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_actorsclient_update_actor: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_binding_capability()
	})
	if checksum != 40861 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_binding_capability: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_binding_name()
	})
	if checksum != 27532 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_binding_name: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_binding_resource()
	})
	if checksum != 48929 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_binding_resource: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_cancellationhandle_cancel()
	})
	if checksum != 39971 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_cancellationhandle_cancel: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_cancellationhandle_is_cancelled()
	})
	if checksum != 56900 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_cancellationhandle_is_cancelled: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_codesha256_value()
	})
	if checksum != 19844 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_codesha256_value: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_positiveu64_value()
	})
	if checksum != 56991 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_positiveu64_value: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_subscriptionspec_placement_anchor()
	})
	if checksum != 48631 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_subscriptionspec_placement_anchor: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_subscriptionspec_stream_path()
	})
	if checksum != 21507 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_subscriptionspec_stream_path: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_method_subscriptionspec_subscription_id()
	})
	if checksum != 19100 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_method_subscriptionspec_subscription_id: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_actorid_new()
	})
	if checksum != 61200 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_actorid_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_actorlimits_new()
	})
	if checksum != 34546 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_actorlimits_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_addsubscriptionrequest_new()
	})
	if checksum != 46402 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_addsubscriptionrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_binding_new()
	})
	if checksum != 24216 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_binding_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_cancellationhandle_new()
	})
	if checksum != 25193 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_cancellationhandle_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_checkpointactorrequest_new()
	})
	if checksum != 63261 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_checkpointactorrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_codesha256_new()
	})
	if checksum != 29565 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_codesha256_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_createactorrequest_new()
	})
	if checksum != 19792 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_createactorrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_inspectactorrequest_new()
	})
	if checksum != 32975 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_inspectactorrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_invokeactorrequest_new()
	})
	if checksum != 46793 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_invokeactorrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_positiveu64_new()
	})
	if checksum != 55625 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_positiveu64_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_removesubscriptionrequest_new()
	})
	if checksum != 37847 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_removesubscriptionrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_resumesubscriptionrequest_new()
	})
	if checksum != 20978 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_resumesubscriptionrequest_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_subscriptionspec_new()
	})
	if checksum != 57449 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_subscriptionspec_new: UniFFI API checksum mismatch")
	}
	}
	{
	checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
		return C.uniffi_acyclic_actors_uniffi_checksum_constructor_updateactorrequest_new()
	})
	if checksum != 11094 {
		// If this happens try cleaning and rebuilding your project
		panic("acyclic_actors_uniffi: uniffi_acyclic_actors_uniffi_checksum_constructor_updateactorrequest_new: UniFFI API checksum mismatch")
	}
	}
}



type FfiConverterUint32 struct{}

var FfiConverterUint32INSTANCE = FfiConverterUint32{}

func (FfiConverterUint32) Lower(value uint32) C.uint32_t {
	return C.uint32_t(value)
}

func (FfiConverterUint32) Write(writer io.Writer, value uint32) {
	writeUint32(writer, value)
}

func (FfiConverterUint32) Lift(value C.uint32_t) uint32 {
	return uint32(value)
}

func (FfiConverterUint32) Read(reader io.Reader) uint32 {
	return readUint32(reader)
}

type FfiDestroyerUint32 struct {}

func (FfiDestroyerUint32) Destroy(_ uint32) {}

type FfiConverterInt32 struct{}

var FfiConverterInt32INSTANCE = FfiConverterInt32{}

func (FfiConverterInt32) Lower(value int32) C.int32_t {
	return C.int32_t(value)
}

func (FfiConverterInt32) Write(writer io.Writer, value int32) {
	writeInt32(writer, value)
}

func (FfiConverterInt32) Lift(value C.int32_t) int32 {
	return int32(value)
}

func (FfiConverterInt32) Read(reader io.Reader) int32 {
	return readInt32(reader)
}

type FfiDestroyerInt32 struct {}

func (FfiDestroyerInt32) Destroy(_ int32) {}

type FfiConverterUint64 struct{}

var FfiConverterUint64INSTANCE = FfiConverterUint64{}

func (FfiConverterUint64) Lower(value uint64) C.uint64_t {
	return C.uint64_t(value)
}

func (FfiConverterUint64) Write(writer io.Writer, value uint64) {
	writeUint64(writer, value)
}

func (FfiConverterUint64) Lift(value C.uint64_t) uint64 {
	return uint64(value)
}

func (FfiConverterUint64) Read(reader io.Reader) uint64 {
	return readUint64(reader)
}

type FfiDestroyerUint64 struct {}

func (FfiDestroyerUint64) Destroy(_ uint64) {}

type FfiConverterBool struct{}

var FfiConverterBoolINSTANCE = FfiConverterBool{}

func (FfiConverterBool) Lower(value bool) C.int8_t {
	if value {
		return C.int8_t(1)
	}
	return C.int8_t(0)
}

func (FfiConverterBool) Write(writer io.Writer, value bool) {
	if value {
		writeInt8(writer, 1)
	} else {
		writeInt8(writer, 0)
	}
}

func (FfiConverterBool) Lift(value C.int8_t) bool {
	return value != 0
}

func (FfiConverterBool) Read(reader io.Reader) bool {
	return readInt8(reader) != 0
}

type FfiDestroyerBool struct {}

func (FfiDestroyerBool) Destroy(_ bool) {}

type FfiConverterString struct{}

var FfiConverterStringINSTANCE = FfiConverterString{}

func (FfiConverterString) Lift(rb RustBufferI) string {
	defer rb.Free()
	reader := rb.AsReader()
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("reading reader: %w", err))
	}
	return string(b)
}

func (FfiConverterString) Read(reader io.Reader) string {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading string, expected %d, read %d", length, read_length))
	}
	return string(buffer)
}

func (FfiConverterString) Lower(value string) C.RustBuffer {
	return stringToRustBuffer(value)
}

func (c FfiConverterString) LowerExternal(value string) ExternalCRustBuffer {
	return RustBufferFromC(stringToRustBuffer(value))
}

func (FfiConverterString) Write(writer io.Writer, value string) {
	if len(value) > math.MaxInt32 {
		panic("String is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := io.WriteString(writer, value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing string, expected %d, written %d", len(value), write_length))
	}
}

type FfiDestroyerString struct {}

func (FfiDestroyerString) Destroy(_ string) {}

type FfiConverterBytes struct{}

var FfiConverterBytesINSTANCE = FfiConverterBytes{}

func (c FfiConverterBytes) Lower(value []byte) C.RustBuffer {
	return LowerIntoRustBuffer[[]byte](c, value)
}

func (c FfiConverterBytes) LowerExternal(value []byte) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterBytes) Write(writer io.Writer, value []byte) {
	if len(value) > math.MaxInt32 {
		panic("[]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := writer.Write(value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing []byte, expected %d, written %d", len(value), write_length))
	}
}

func (c FfiConverterBytes) Lift(rb RustBufferI) []byte {
	return LiftFromRustBuffer[[]byte](c, rb)
}

func (c FfiConverterBytes) Read(reader io.Reader) []byte {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading []byte, expected %d, read %d", length, read_length))
	}
	return buffer
}

type FfiDestroyerBytes struct {}

func (FfiDestroyerBytes) Destroy(_ []byte) {}



// Below is an implementation of synchronization requirements outlined in the link.
// https://github.com/mozilla/uniffi-rs/blob/0dc031132d9493ca812c3af6e7dd60ad2ea95bf0/uniffi_bindgen/src/bindings/kotlin/templates/ObjectRuntime.kt#L31

type FfiObject struct {
	handle C.uint64_t
	callCounter atomic.Int64
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t
	freeFunction func(C.uint64_t, *C.RustCallStatus)
	destroyed atomic.Bool
}

func newFfiObject(
	handle C.uint64_t,
	cloneFunction func(C.uint64_t, *C.RustCallStatus) C.uint64_t,
	freeFunction func(C.uint64_t, *C.RustCallStatus),
) FfiObject {
	return FfiObject {
		handle: handle,
		cloneFunction: cloneFunction,
		freeFunction: freeFunction,
	}
}

func (ffiObject *FfiObject)incrementPointer(debugName string) C.uint64_t {
	for {
		counter := ffiObject.callCounter.Load()
		if counter <= -1 {
			panic(fmt.Errorf("%v object has already been destroyed", debugName))
		}
		if counter == math.MaxInt64 {
			panic(fmt.Errorf("%v object call counter would overflow", debugName))
		}
		if ffiObject.callCounter.CompareAndSwap(counter, counter + 1) {
			break
		}
	}

	return rustCall(func(status *C.RustCallStatus) C.uint64_t {
		return ffiObject.cloneFunction(ffiObject.handle, status)
	})
}

func (ffiObject *FfiObject)decrementPointer() {
	if ffiObject.callCounter.Add(-1) == -1 {
		ffiObject.freeRustArcPtr()
	}
}

func (ffiObject *FfiObject)destroy() {
	if ffiObject.destroyed.CompareAndSwap(false, true) {
		if ffiObject.callCounter.Add(-1) == -1 {
			ffiObject.freeRustArcPtr()
		}
	}
}

func (ffiObject *FfiObject)freeRustArcPtr() {
	if ffiObject.handle == 0 {
		return
	}
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.handle, status)
		return 0
	})
}
// A Rust-owned non-empty Actor identity.
type ActorIdInterface interface {
	// Returns the exact wire spelling.
	Value() string
}
// A Rust-owned non-empty Actor identity.
type ActorId struct {
	ffiObject FfiObject
}
// Constructs an Actor identity through the canonical Rust domain type.
func NewActorId(value string) (*ActorId, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_actorid_new(FfiConverterStringINSTANCE.Lower(value),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *ActorId
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterActorIdINSTANCE.Lift(_uniffiRV), nil
		}
}




// Returns the exact wire spelling.
func (_self *ActorId) Value() string {
	_pointer := _self.ffiObject.incrementPointer("*ActorId")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_actorid_value(
		_pointer,_uniffiStatus),
	}
	}))
}
func (object *ActorId) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterActorId struct {}

var FfiConverterActorIdINSTANCE = FfiConverterActorId{}


func (c FfiConverterActorId) Lift(handle C.uint64_t) *ActorId {
	result := &ActorId {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_actorid(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_actorid(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*ActorId).Destroy)
	return result
}

func (c FfiConverterActorId) Read(reader io.Reader) *ActorId {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterActorId) Lower(value *ActorId) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*ActorId")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterActorId) Write(writer io.Writer, value *ActorId) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalActorId(handle uint64) *ActorId {
	return FfiConverterActorIdINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalActorId(value *ActorId) uint64 {
	return uint64(FfiConverterActorIdINSTANCE.Lower(value))
}

type FfiDestroyerActorId struct {}

func (_ FfiDestroyerActorId) Destroy(value *ActorId) {
		value.Destroy()
}



// Rust-owned positive Actor resource limits.
type ActorLimitsInterface interface {
	CheckpointBytes() uint64
	HandlerTimeoutMillis() uint64
	MemoryBytes() uint64
}
// Rust-owned positive Actor resource limits.
type ActorLimits struct {
	ffiObject FfiObject
}
func NewActorLimits(handlerTimeoutMillis uint64, memoryBytes uint64, checkpointBytes uint64) (*ActorLimits, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_actorlimits_new(FfiConverterUint64INSTANCE.Lower(handlerTimeoutMillis), FfiConverterUint64INSTANCE.Lower(memoryBytes), FfiConverterUint64INSTANCE.Lower(checkpointBytes),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *ActorLimits
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterActorLimitsINSTANCE.Lift(_uniffiRV), nil
		}
}




func (_self *ActorLimits) CheckpointBytes() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*ActorLimits")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_method_actorlimits_checkpoint_bytes(
		_pointer,_uniffiStatus)
	}))
}

func (_self *ActorLimits) HandlerTimeoutMillis() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*ActorLimits")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_method_actorlimits_handler_timeout_millis(
		_pointer,_uniffiStatus)
	}))
}

func (_self *ActorLimits) MemoryBytes() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*ActorLimits")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_method_actorlimits_memory_bytes(
		_pointer,_uniffiStatus)
	}))
}
func (object *ActorLimits) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterActorLimits struct {}

var FfiConverterActorLimitsINSTANCE = FfiConverterActorLimits{}


func (c FfiConverterActorLimits) Lift(handle C.uint64_t) *ActorLimits {
	result := &ActorLimits {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_actorlimits(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_actorlimits(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*ActorLimits).Destroy)
	return result
}

func (c FfiConverterActorLimits) Read(reader io.Reader) *ActorLimits {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterActorLimits) Lower(value *ActorLimits) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*ActorLimits")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterActorLimits) Write(writer io.Writer, value *ActorLimits) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalActorLimits(handle uint64) *ActorLimits {
	return FfiConverterActorLimitsINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalActorLimits(value *ActorLimits) uint64 {
	return uint64(FfiConverterActorLimitsINSTANCE.Lower(value))
}

type FfiDestroyerActorLimits struct {}

func (_ FfiDestroyerActorLimits) Destroy(value *ActorLimits) {
		value.Destroy()
}



// Opaque Rust-owned Actors client.
type ActorsClientInterface interface {
	AddSubscription(request *AddSubscriptionRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	CheckpointActor(request *CheckpointActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	CreateActor(request *CreateActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	// Executes the legacy inspect operation through the canonical Rust client.
	InspectActor(actorId *ActorId, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	InspectActorRequest(request *InspectActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	InvokeActor(request *InvokeActorRequest, cancellation **CancellationHandle, ctx context.Context) (InvokeActorResponse, error)
	RemoveSubscription(request *RemoveSubscriptionRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	ResumeSubscription(request *ResumeSubscriptionRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
	UpdateActor(request *UpdateActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error)
}
// Opaque Rust-owned Actors client.
type ActorsClient struct {
	ffiObject FfiObject
}




func (_self *ActorsClient) AddSubscription(request *AddSubscriptionRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_add_subscription(
		_pointer,FfiConverterAddSubscriptionRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) CheckpointActor(request *CheckpointActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_checkpoint_actor(
		_pointer,FfiConverterCheckpointActorRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) CreateActor(request *CreateActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_create_actor(
		_pointer,FfiConverterCreateActorRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

// Executes the legacy inspect operation through the canonical Rust client.
func (_self *ActorsClient) InspectActor(actorId *ActorId, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_inspect_actor(
		_pointer,FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) InspectActorRequest(request *InspectActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_inspect_actor_request(
		_pointer,FfiConverterInspectActorRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) InvokeActor(request *InvokeActorRequest, cancellation **CancellationHandle, ctx context.Context) (InvokeActorResponse, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) InvokeActorResponse {
			return FfiConverterInvokeActorResponseINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_invoke_actor(
		_pointer,FfiConverterInvokeActorRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) RemoveSubscription(request *RemoveSubscriptionRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_remove_subscription(
		_pointer,FfiConverterRemoveSubscriptionRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) ResumeSubscription(request *ResumeSubscriptionRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_resume_subscription(
		_pointer,FfiConverterResumeSubscriptionRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

func (_self *ActorsClient) UpdateActor(request *UpdateActorRequest, cancellation **CancellationHandle, ctx context.Context) (*ActorObservation, error) {
	_pointer := _self.ffiObject.incrementPointer("*ActorsClient")
	defer _self.ffiObject.decrementPointer()
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_rust_buffer(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) RustBufferI {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_rust_buffer(handle, status)
			return GoRustBuffer {
		inner: res,
	}
		},
		// liftFn
		func(ffi RustBufferI) *ActorObservation {
			return FfiConverterOptionalActorObservationINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_method_actorsclient_update_actor(
		_pointer,FfiConverterUpdateActorRequestINSTANCE.Lower(request), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_rust_buffer(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_rust_buffer(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}
func (object *ActorsClient) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterActorsClient struct {}

var FfiConverterActorsClientINSTANCE = FfiConverterActorsClient{}


func (c FfiConverterActorsClient) Lift(handle C.uint64_t) *ActorsClient {
	result := &ActorsClient {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_actorsclient(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_actorsclient(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*ActorsClient).Destroy)
	return result
}

func (c FfiConverterActorsClient) Read(reader io.Reader) *ActorsClient {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterActorsClient) Lower(value *ActorsClient) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*ActorsClient")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterActorsClient) Write(writer io.Writer, value *ActorsClient) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalActorsClient(handle uint64) *ActorsClient {
	return FfiConverterActorsClientINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalActorsClient(value *ActorsClient) uint64 {
	return uint64(FfiConverterActorsClientINSTANCE.Lower(value))
}

type FfiDestroyerActorsClient struct {}

func (_ FfiDestroyerActorsClient) Destroy(value *ActorsClient) {
		value.Destroy()
}



// Opaque validated add-subscription request.
type AddSubscriptionRequestInterface interface {
}
// Opaque validated add-subscription request.
type AddSubscriptionRequest struct {
	ffiObject FfiObject
}
func NewAddSubscriptionRequest(actorId *ActorId, subscription *SubscriptionSpec, idempotencyKey string) (*AddSubscriptionRequest, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_addsubscriptionrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterSubscriptionSpecINSTANCE.Lower(subscription), FfiConverterStringINSTANCE.Lower(idempotencyKey),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *AddSubscriptionRequest
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterAddSubscriptionRequestINSTANCE.Lift(_uniffiRV), nil
		}
}



func (object *AddSubscriptionRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterAddSubscriptionRequest struct {}

var FfiConverterAddSubscriptionRequestINSTANCE = FfiConverterAddSubscriptionRequest{}


func (c FfiConverterAddSubscriptionRequest) Lift(handle C.uint64_t) *AddSubscriptionRequest {
	result := &AddSubscriptionRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_addsubscriptionrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_addsubscriptionrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*AddSubscriptionRequest).Destroy)
	return result
}

func (c FfiConverterAddSubscriptionRequest) Read(reader io.Reader) *AddSubscriptionRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterAddSubscriptionRequest) Lower(value *AddSubscriptionRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*AddSubscriptionRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterAddSubscriptionRequest) Write(writer io.Writer, value *AddSubscriptionRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalAddSubscriptionRequest(handle uint64) *AddSubscriptionRequest {
	return FfiConverterAddSubscriptionRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalAddSubscriptionRequest(value *AddSubscriptionRequest) uint64 {
	return uint64(FfiConverterAddSubscriptionRequestINSTANCE.Lower(value))
}

type FfiDestroyerAddSubscriptionRequest struct {}

func (_ FfiDestroyerAddSubscriptionRequest) Destroy(value *AddSubscriptionRequest) {
		value.Destroy()
}



// Rust-owned resource binding. The opaque object contains the canonical
// validated domain value; foreign callers cannot construct an invalid binding.
type BindingInterface interface {
	Capability() string
	Name() string
	Resource() string
}
// Rust-owned resource binding. The opaque object contains the canonical
// validated domain value; foreign callers cannot construct an invalid binding.
type Binding struct {
	ffiObject FfiObject
}
func NewBinding(name string, capability string, resource string) (*Binding, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_binding_new(FfiConverterStringINSTANCE.Lower(name), FfiConverterStringINSTANCE.Lower(capability), FfiConverterStringINSTANCE.Lower(resource),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *Binding
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterBindingINSTANCE.Lift(_uniffiRV), nil
		}
}




func (_self *Binding) Capability() string {
	_pointer := _self.ffiObject.incrementPointer("*Binding")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_binding_capability(
		_pointer,_uniffiStatus),
	}
	}))
}

func (_self *Binding) Name() string {
	_pointer := _self.ffiObject.incrementPointer("*Binding")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_binding_name(
		_pointer,_uniffiStatus),
	}
	}))
}

func (_self *Binding) Resource() string {
	_pointer := _self.ffiObject.incrementPointer("*Binding")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_binding_resource(
		_pointer,_uniffiStatus),
	}
	}))
}
func (object *Binding) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterBinding struct {}

var FfiConverterBindingINSTANCE = FfiConverterBinding{}


func (c FfiConverterBinding) Lift(handle C.uint64_t) *Binding {
	result := &Binding {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_binding(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_binding(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Binding).Destroy)
	return result
}

func (c FfiConverterBinding) Read(reader io.Reader) *Binding {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterBinding) Lower(value *Binding) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*Binding")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterBinding) Write(writer io.Writer, value *Binding) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalBinding(handle uint64) *Binding {
	return FfiConverterBindingINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalBinding(value *Binding) uint64 {
	return uint64(FfiConverterBindingINSTANCE.Lower(value))
}

type FfiDestroyerBinding struct {}

func (_ FfiDestroyerBinding) Destroy(value *Binding) {
		value.Destroy()
}



// Explicit Rust-owned cancellation for an in-flight Actors operation.
type CancellationHandleInterface interface {
	// Cancels the Rust-owned operation associated with this handle.
	Cancel() 
	// Returns whether cancellation has been requested.
	IsCancelled() bool
}
// Explicit Rust-owned cancellation for an in-flight Actors operation.
type CancellationHandle struct {
	ffiObject FfiObject
}
// Creates a fresh, one-shot cancellation handle.
func NewCancellationHandle() *CancellationHandle {
	return FfiConverterCancellationHandleINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_cancellationhandle_new(_uniffiStatus)
	}))
}




// Cancels the Rust-owned operation associated with this handle.
func (_self *CancellationHandle) Cancel()  {
	_pointer := _self.ffiObject.incrementPointer("*CancellationHandle")
	defer _self.ffiObject.decrementPointer()
	rustCall(func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_acyclic_actors_uniffi_fn_method_cancellationhandle_cancel(
		_pointer,_uniffiStatus)
		return false
	})
}

// Returns whether cancellation has been requested.
func (_self *CancellationHandle) IsCancelled() bool {
	_pointer := _self.ffiObject.incrementPointer("*CancellationHandle")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBoolINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.int8_t {
		return C.uniffi_acyclic_actors_uniffi_fn_method_cancellationhandle_is_cancelled(
		_pointer,_uniffiStatus)
	}))
}
func (object *CancellationHandle) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterCancellationHandle struct {}

var FfiConverterCancellationHandleINSTANCE = FfiConverterCancellationHandle{}


func (c FfiConverterCancellationHandle) Lift(handle C.uint64_t) *CancellationHandle {
	result := &CancellationHandle {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_cancellationhandle(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_cancellationhandle(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*CancellationHandle).Destroy)
	return result
}

func (c FfiConverterCancellationHandle) Read(reader io.Reader) *CancellationHandle {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterCancellationHandle) Lower(value *CancellationHandle) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*CancellationHandle")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterCancellationHandle) Write(writer io.Writer, value *CancellationHandle) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalCancellationHandle(handle uint64) *CancellationHandle {
	return FfiConverterCancellationHandleINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalCancellationHandle(value *CancellationHandle) uint64 {
	return uint64(FfiConverterCancellationHandleINSTANCE.Lower(value))
}

type FfiDestroyerCancellationHandle struct {}

func (_ FfiDestroyerCancellationHandle) Destroy(value *CancellationHandle) {
		value.Destroy()
}



// Opaque checkpoint request.
type CheckpointActorRequestInterface interface {
}
// Opaque checkpoint request.
type CheckpointActorRequest struct {
	ffiObject FfiObject
}
func NewCheckpointActorRequest(actorId *ActorId, idempotencyKey string) *CheckpointActorRequest {
	return FfiConverterCheckpointActorRequestINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_checkpointactorrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterStringINSTANCE.Lower(idempotencyKey),_uniffiStatus)
	}))
}



func (object *CheckpointActorRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterCheckpointActorRequest struct {}

var FfiConverterCheckpointActorRequestINSTANCE = FfiConverterCheckpointActorRequest{}


func (c FfiConverterCheckpointActorRequest) Lift(handle C.uint64_t) *CheckpointActorRequest {
	result := &CheckpointActorRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_checkpointactorrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_checkpointactorrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*CheckpointActorRequest).Destroy)
	return result
}

func (c FfiConverterCheckpointActorRequest) Read(reader io.Reader) *CheckpointActorRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterCheckpointActorRequest) Lower(value *CheckpointActorRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*CheckpointActorRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterCheckpointActorRequest) Write(writer io.Writer, value *CheckpointActorRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalCheckpointActorRequest(handle uint64) *CheckpointActorRequest {
	return FfiConverterCheckpointActorRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalCheckpointActorRequest(value *CheckpointActorRequest) uint64 {
	return uint64(FfiConverterCheckpointActorRequestINSTANCE.Lower(value))
}

type FfiDestroyerCheckpointActorRequest struct {}

func (_ FfiDestroyerCheckpointActorRequest) Destroy(value *CheckpointActorRequest) {
		value.Destroy()
}



// A Rust-owned non-zero 32-byte SHA-256 digest.
type CodeSha256Interface interface {
	// Returns the exact digest bytes.
	Value() []byte
}
// A Rust-owned non-zero 32-byte SHA-256 digest.
type CodeSha256 struct {
	ffiObject FfiObject
}
// Constructs a digest through the canonical Rust domain type.
func NewCodeSha256(value []byte) (*CodeSha256, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_codesha256_new(FfiConverterBytesINSTANCE.Lower(value),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *CodeSha256
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterCodeSha256INSTANCE.Lift(_uniffiRV), nil
		}
}




// Returns the exact digest bytes.
func (_self *CodeSha256) Value() []byte {
	_pointer := _self.ffiObject.incrementPointer("*CodeSha256")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBytesINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_codesha256_value(
		_pointer,_uniffiStatus),
	}
	}))
}
func (object *CodeSha256) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterCodeSha256 struct {}

var FfiConverterCodeSha256INSTANCE = FfiConverterCodeSha256{}


func (c FfiConverterCodeSha256) Lift(handle C.uint64_t) *CodeSha256 {
	result := &CodeSha256 {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_codesha256(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_codesha256(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*CodeSha256).Destroy)
	return result
}

func (c FfiConverterCodeSha256) Read(reader io.Reader) *CodeSha256 {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterCodeSha256) Lower(value *CodeSha256) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*CodeSha256")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterCodeSha256) Write(writer io.Writer, value *CodeSha256) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalCodeSha256(handle uint64) *CodeSha256 {
	return FfiConverterCodeSha256INSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalCodeSha256(value *CodeSha256) uint64 {
	return uint64(FfiConverterCodeSha256INSTANCE.Lower(value))
}

type FfiDestroyerCodeSha256 struct {}

func (_ FfiDestroyerCodeSha256) Destroy(value *CodeSha256) {
		value.Destroy()
}



// Opaque validated create request.
type CreateActorRequestInterface interface {
}
// Opaque validated create request.
type CreateActorRequest struct {
	ffiObject FfiObject
}
func NewCreateActorRequest(codeSha256 *CodeSha256, homeRegion string, bindings []*Binding, limits *ActorLimits, subscriptions []*SubscriptionSpec, idempotencyKey string) (*CreateActorRequest, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_createactorrequest_new(FfiConverterCodeSha256INSTANCE.Lower(codeSha256), FfiConverterStringINSTANCE.Lower(homeRegion), FfiConverterSequenceBindingINSTANCE.Lower(bindings), FfiConverterActorLimitsINSTANCE.Lower(limits), FfiConverterSequenceSubscriptionSpecINSTANCE.Lower(subscriptions), FfiConverterStringINSTANCE.Lower(idempotencyKey),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *CreateActorRequest
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterCreateActorRequestINSTANCE.Lift(_uniffiRV), nil
		}
}



func (object *CreateActorRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterCreateActorRequest struct {}

var FfiConverterCreateActorRequestINSTANCE = FfiConverterCreateActorRequest{}


func (c FfiConverterCreateActorRequest) Lift(handle C.uint64_t) *CreateActorRequest {
	result := &CreateActorRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_createactorrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_createactorrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*CreateActorRequest).Destroy)
	return result
}

func (c FfiConverterCreateActorRequest) Read(reader io.Reader) *CreateActorRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterCreateActorRequest) Lower(value *CreateActorRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*CreateActorRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterCreateActorRequest) Write(writer io.Writer, value *CreateActorRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalCreateActorRequest(handle uint64) *CreateActorRequest {
	return FfiConverterCreateActorRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalCreateActorRequest(value *CreateActorRequest) uint64 {
	return uint64(FfiConverterCreateActorRequestINSTANCE.Lower(value))
}

type FfiDestroyerCreateActorRequest struct {}

func (_ FfiDestroyerCreateActorRequest) Destroy(value *CreateActorRequest) {
		value.Destroy()
}



// Opaque inspect request. The legacy ActorId overload remains on ActorsClient.
type InspectActorRequestInterface interface {
}
// Opaque inspect request. The legacy ActorId overload remains on ActorsClient.
type InspectActorRequest struct {
	ffiObject FfiObject
}
func NewInspectActorRequest(actorId *ActorId) *InspectActorRequest {
	return FfiConverterInspectActorRequestINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_inspectactorrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId),_uniffiStatus)
	}))
}



func (object *InspectActorRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterInspectActorRequest struct {}

var FfiConverterInspectActorRequestINSTANCE = FfiConverterInspectActorRequest{}


func (c FfiConverterInspectActorRequest) Lift(handle C.uint64_t) *InspectActorRequest {
	result := &InspectActorRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_inspectactorrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_inspectactorrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*InspectActorRequest).Destroy)
	return result
}

func (c FfiConverterInspectActorRequest) Read(reader io.Reader) *InspectActorRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterInspectActorRequest) Lower(value *InspectActorRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*InspectActorRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterInspectActorRequest) Write(writer io.Writer, value *InspectActorRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalInspectActorRequest(handle uint64) *InspectActorRequest {
	return FfiConverterInspectActorRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalInspectActorRequest(value *InspectActorRequest) uint64 {
	return uint64(FfiConverterInspectActorRequestINSTANCE.Lower(value))
}

type FfiDestroyerInspectActorRequest struct {}

func (_ FfiDestroyerInspectActorRequest) Destroy(value *InspectActorRequest) {
		value.Destroy()
}



// Opaque invocation request.
type InvokeActorRequestInterface interface {
}
// Opaque invocation request.
type InvokeActorRequest struct {
	ffiObject FfiObject
}
func NewInvokeActorRequest(actorId *ActorId, method string, url string, body []byte, headers []Header) *InvokeActorRequest {
	return FfiConverterInvokeActorRequestINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_invokeactorrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterStringINSTANCE.Lower(method), FfiConverterStringINSTANCE.Lower(url), FfiConverterBytesINSTANCE.Lower(body), FfiConverterSequenceHeaderINSTANCE.Lower(headers),_uniffiStatus)
	}))
}



func (object *InvokeActorRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterInvokeActorRequest struct {}

var FfiConverterInvokeActorRequestINSTANCE = FfiConverterInvokeActorRequest{}


func (c FfiConverterInvokeActorRequest) Lift(handle C.uint64_t) *InvokeActorRequest {
	result := &InvokeActorRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_invokeactorrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_invokeactorrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*InvokeActorRequest).Destroy)
	return result
}

func (c FfiConverterInvokeActorRequest) Read(reader io.Reader) *InvokeActorRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterInvokeActorRequest) Lower(value *InvokeActorRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*InvokeActorRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterInvokeActorRequest) Write(writer io.Writer, value *InvokeActorRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalInvokeActorRequest(handle uint64) *InvokeActorRequest {
	return FfiConverterInvokeActorRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalInvokeActorRequest(value *InvokeActorRequest) uint64 {
	return uint64(FfiConverterInvokeActorRequestINSTANCE.Lower(value))
}

type FfiDestroyerInvokeActorRequest struct {}

func (_ FfiDestroyerInvokeActorRequest) Destroy(value *InvokeActorRequest) {
		value.Destroy()
}



// A Rust-owned strictly positive unsigned 64-bit value.
type PositiveU64Interface interface {
	// Returns the exact unsigned value.
	Value() uint64
}
// A Rust-owned strictly positive unsigned 64-bit value.
type PositiveU64 struct {
	ffiObject FfiObject
}
// Constructs a positive value through the canonical Rust domain type.
func NewPositiveU64(value uint64) (*PositiveU64, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_positiveu64_new(FfiConverterUint64INSTANCE.Lower(value),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *PositiveU64
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterPositiveU64INSTANCE.Lift(_uniffiRV), nil
		}
}




// Returns the exact unsigned value.
func (_self *PositiveU64) Value() uint64 {
	_pointer := _self.ffiObject.incrementPointer("*PositiveU64")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterUint64INSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_method_positiveu64_value(
		_pointer,_uniffiStatus)
	}))
}
func (object *PositiveU64) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterPositiveU64 struct {}

var FfiConverterPositiveU64INSTANCE = FfiConverterPositiveU64{}


func (c FfiConverterPositiveU64) Lift(handle C.uint64_t) *PositiveU64 {
	result := &PositiveU64 {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_positiveu64(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_positiveu64(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*PositiveU64).Destroy)
	return result
}

func (c FfiConverterPositiveU64) Read(reader io.Reader) *PositiveU64 {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterPositiveU64) Lower(value *PositiveU64) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*PositiveU64")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterPositiveU64) Write(writer io.Writer, value *PositiveU64) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalPositiveU64(handle uint64) *PositiveU64 {
	return FfiConverterPositiveU64INSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalPositiveU64(value *PositiveU64) uint64 {
	return uint64(FfiConverterPositiveU64INSTANCE.Lower(value))
}

type FfiDestroyerPositiveU64 struct {}

func (_ FfiDestroyerPositiveU64) Destroy(value *PositiveU64) {
		value.Destroy()
}



// Opaque remove-subscription request.
type RemoveSubscriptionRequestInterface interface {
}
// Opaque remove-subscription request.
type RemoveSubscriptionRequest struct {
	ffiObject FfiObject
}
func NewRemoveSubscriptionRequest(actorId *ActorId, subscriptionId string, idempotencyKey string) *RemoveSubscriptionRequest {
	return FfiConverterRemoveSubscriptionRequestINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_removesubscriptionrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterStringINSTANCE.Lower(subscriptionId), FfiConverterStringINSTANCE.Lower(idempotencyKey),_uniffiStatus)
	}))
}



func (object *RemoveSubscriptionRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterRemoveSubscriptionRequest struct {}

var FfiConverterRemoveSubscriptionRequestINSTANCE = FfiConverterRemoveSubscriptionRequest{}


func (c FfiConverterRemoveSubscriptionRequest) Lift(handle C.uint64_t) *RemoveSubscriptionRequest {
	result := &RemoveSubscriptionRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_removesubscriptionrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_removesubscriptionrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*RemoveSubscriptionRequest).Destroy)
	return result
}

func (c FfiConverterRemoveSubscriptionRequest) Read(reader io.Reader) *RemoveSubscriptionRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterRemoveSubscriptionRequest) Lower(value *RemoveSubscriptionRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*RemoveSubscriptionRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterRemoveSubscriptionRequest) Write(writer io.Writer, value *RemoveSubscriptionRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalRemoveSubscriptionRequest(handle uint64) *RemoveSubscriptionRequest {
	return FfiConverterRemoveSubscriptionRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalRemoveSubscriptionRequest(value *RemoveSubscriptionRequest) uint64 {
	return uint64(FfiConverterRemoveSubscriptionRequestINSTANCE.Lower(value))
}

type FfiDestroyerRemoveSubscriptionRequest struct {}

func (_ FfiDestroyerRemoveSubscriptionRequest) Destroy(value *RemoveSubscriptionRequest) {
		value.Destroy()
}



// Opaque resume-subscription request.
type ResumeSubscriptionRequestInterface interface {
}
// Opaque resume-subscription request.
type ResumeSubscriptionRequest struct {
	ffiObject FfiObject
}
func NewResumeSubscriptionRequest(actorId *ActorId, subscriptionId string, idempotencyKey string) *ResumeSubscriptionRequest {
	return FfiConverterResumeSubscriptionRequestINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_resumesubscriptionrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterStringINSTANCE.Lower(subscriptionId), FfiConverterStringINSTANCE.Lower(idempotencyKey),_uniffiStatus)
	}))
}



func (object *ResumeSubscriptionRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterResumeSubscriptionRequest struct {}

var FfiConverterResumeSubscriptionRequestINSTANCE = FfiConverterResumeSubscriptionRequest{}


func (c FfiConverterResumeSubscriptionRequest) Lift(handle C.uint64_t) *ResumeSubscriptionRequest {
	result := &ResumeSubscriptionRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_resumesubscriptionrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_resumesubscriptionrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*ResumeSubscriptionRequest).Destroy)
	return result
}

func (c FfiConverterResumeSubscriptionRequest) Read(reader io.Reader) *ResumeSubscriptionRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterResumeSubscriptionRequest) Lower(value *ResumeSubscriptionRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*ResumeSubscriptionRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterResumeSubscriptionRequest) Write(writer io.Writer, value *ResumeSubscriptionRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalResumeSubscriptionRequest(handle uint64) *ResumeSubscriptionRequest {
	return FfiConverterResumeSubscriptionRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalResumeSubscriptionRequest(value *ResumeSubscriptionRequest) uint64 {
	return uint64(FfiConverterResumeSubscriptionRequestINSTANCE.Lower(value))
}

type FfiDestroyerResumeSubscriptionRequest struct {}

func (_ FfiDestroyerResumeSubscriptionRequest) Destroy(value *ResumeSubscriptionRequest) {
		value.Destroy()
}



// Rust-owned validated subscription specification.
type SubscriptionSpecInterface interface {
	PlacementAnchor() bool
	StreamPath() string
	SubscriptionId() string
}
// Rust-owned validated subscription specification.
type SubscriptionSpec struct {
	ffiObject FfiObject
}
func NewSubscriptionSpec(subscriptionId string, streamPath string, start SubscriptionStart, placementAnchor bool) (*SubscriptionSpec, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_subscriptionspec_new(FfiConverterStringINSTANCE.Lower(subscriptionId), FfiConverterStringINSTANCE.Lower(streamPath), FfiConverterSubscriptionStartINSTANCE.Lower(start), FfiConverterBoolINSTANCE.Lower(placementAnchor),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *SubscriptionSpec
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterSubscriptionSpecINSTANCE.Lift(_uniffiRV), nil
		}
}




func (_self *SubscriptionSpec) PlacementAnchor() bool {
	_pointer := _self.ffiObject.incrementPointer("*SubscriptionSpec")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterBoolINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) C.int8_t {
		return C.uniffi_acyclic_actors_uniffi_fn_method_subscriptionspec_placement_anchor(
		_pointer,_uniffiStatus)
	}))
}

func (_self *SubscriptionSpec) StreamPath() string {
	_pointer := _self.ffiObject.incrementPointer("*SubscriptionSpec")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_subscriptionspec_stream_path(
		_pointer,_uniffiStatus),
	}
	}))
}

func (_self *SubscriptionSpec) SubscriptionId() string {
	_pointer := _self.ffiObject.incrementPointer("*SubscriptionSpec")
	defer _self.ffiObject.decrementPointer()
	return FfiConverterStringINSTANCE.Lift(rustCall(func(_uniffiStatus *C.RustCallStatus) RustBufferI {
		return GoRustBuffer {
		inner: C.uniffi_acyclic_actors_uniffi_fn_method_subscriptionspec_subscription_id(
		_pointer,_uniffiStatus),
	}
	}))
}
func (object *SubscriptionSpec) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterSubscriptionSpec struct {}

var FfiConverterSubscriptionSpecINSTANCE = FfiConverterSubscriptionSpec{}


func (c FfiConverterSubscriptionSpec) Lift(handle C.uint64_t) *SubscriptionSpec {
	result := &SubscriptionSpec {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_subscriptionspec(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_subscriptionspec(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*SubscriptionSpec).Destroy)
	return result
}

func (c FfiConverterSubscriptionSpec) Read(reader io.Reader) *SubscriptionSpec {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterSubscriptionSpec) Lower(value *SubscriptionSpec) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*SubscriptionSpec")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterSubscriptionSpec) Write(writer io.Writer, value *SubscriptionSpec) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalSubscriptionSpec(handle uint64) *SubscriptionSpec {
	return FfiConverterSubscriptionSpecINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalSubscriptionSpec(value *SubscriptionSpec) uint64 {
	return uint64(FfiConverterSubscriptionSpecINSTANCE.Lower(value))
}

type FfiDestroyerSubscriptionSpec struct {}

func (_ FfiDestroyerSubscriptionSpec) Destroy(value *SubscriptionSpec) {
		value.Destroy()
}



// Opaque validated update request.
type UpdateActorRequestInterface interface {
}
// Opaque validated update request.
type UpdateActorRequest struct {
	ffiObject FfiObject
}
func NewUpdateActorRequest(actorId *ActorId, codeSha256 *CodeSha256, bindings []*Binding, limits *ActorLimits, expectedConfigurationRevision uint64, idempotencyKey string) (*UpdateActorRequest, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[*BindingError](FfiConverterBindingError{},func(_uniffiStatus *C.RustCallStatus) C.uint64_t {
		return C.uniffi_acyclic_actors_uniffi_fn_constructor_updateactorrequest_new(FfiConverterActorIdINSTANCE.Lower(actorId), FfiConverterCodeSha256INSTANCE.Lower(codeSha256), FfiConverterSequenceBindingINSTANCE.Lower(bindings), FfiConverterActorLimitsINSTANCE.Lower(limits), FfiConverterUint64INSTANCE.Lower(expectedConfigurationRevision), FfiConverterStringINSTANCE.Lower(idempotencyKey),_uniffiStatus)
	})
		if _uniffiErr != nil {
			var _uniffiDefaultValue *UpdateActorRequest
			return _uniffiDefaultValue, _uniffiErr
		} else {
			return FfiConverterUpdateActorRequestINSTANCE.Lift(_uniffiRV), nil
		}
}



func (object *UpdateActorRequest) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterUpdateActorRequest struct {}

var FfiConverterUpdateActorRequestINSTANCE = FfiConverterUpdateActorRequest{}


func (c FfiConverterUpdateActorRequest) Lift(handle C.uint64_t) *UpdateActorRequest {
	result := &UpdateActorRequest {
		newFfiObject(
			handle,
			func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
				return C.uniffi_acyclic_actors_uniffi_fn_clone_updateactorrequest(handle, status)
			},
			func(handle C.uint64_t, status *C.RustCallStatus) {
				C.uniffi_acyclic_actors_uniffi_fn_free_updateactorrequest(handle, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*UpdateActorRequest).Destroy)
	return result
}

func (c FfiConverterUpdateActorRequest) Read(reader io.Reader) *UpdateActorRequest {
	return c.Lift(C.uint64_t(readUint64(reader)))
}

func (c FfiConverterUpdateActorRequest) Lower(value *UpdateActorRequest) C.uint64_t {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the handle will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked handle.
	handle := value.ffiObject.incrementPointer("*UpdateActorRequest")
	defer value.ffiObject.decrementPointer()
	return handle
}

func (c FfiConverterUpdateActorRequest) Write(writer io.Writer, value *UpdateActorRequest) {
	writeUint64(writer, uint64(c.Lower(value)))
}

func LiftFromExternalUpdateActorRequest(handle uint64) *UpdateActorRequest {
	return FfiConverterUpdateActorRequestINSTANCE.Lift(C.uint64_t(handle))
}

func LowerToExternalUpdateActorRequest(value *UpdateActorRequest) uint64 {
	return uint64(FfiConverterUpdateActorRequestINSTANCE.Lower(value))
}

type FfiDestroyerUpdateActorRequest struct {}

func (_ FfiDestroyerUpdateActorRequest) Destroy(value *UpdateActorRequest) {
		value.Destroy()
}



// Strongly typed projection of the canonical Actor observation.
type ActorObservation struct {
	ActorId *ActorId
	CodeSha256 *CodeSha256
	HomeRegion string
	State ActorState
	Subscriptions []SubscriptionObservation
	CheckpointUnixMillis *uint64
	CheckpointEpoch uint64
	ConfigurationRevision uint64
}

func (r *ActorObservation) Destroy() {
		FfiDestroyerActorId{}.Destroy(r.ActorId);
		FfiDestroyerCodeSha256{}.Destroy(r.CodeSha256);
		FfiDestroyerString{}.Destroy(r.HomeRegion);
		FfiDestroyerActorState{}.Destroy(r.State);
		FfiDestroyerSequenceSubscriptionObservation{}.Destroy(r.Subscriptions);
		FfiDestroyerOptionalUint64{}.Destroy(r.CheckpointUnixMillis);
		FfiDestroyerUint64{}.Destroy(r.CheckpointEpoch);
		FfiDestroyerUint64{}.Destroy(r.ConfigurationRevision);
}

type FfiConverterActorObservation struct {}

var FfiConverterActorObservationINSTANCE = FfiConverterActorObservation{}

func (c FfiConverterActorObservation) Lift(rb RustBufferI) ActorObservation {
	return LiftFromRustBuffer[ActorObservation](c, rb)
}

func (c FfiConverterActorObservation) Read(reader io.Reader) ActorObservation {
	return ActorObservation {
			FfiConverterActorIdINSTANCE.Read(reader),
			FfiConverterCodeSha256INSTANCE.Read(reader),
			FfiConverterStringINSTANCE.Read(reader),
			FfiConverterActorStateINSTANCE.Read(reader),
			FfiConverterSequenceSubscriptionObservationINSTANCE.Read(reader),
			FfiConverterOptionalUint64INSTANCE.Read(reader),
			FfiConverterUint64INSTANCE.Read(reader),
			FfiConverterUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterActorObservation) Lower(value ActorObservation) C.RustBuffer {
	return LowerIntoRustBuffer[ActorObservation](c, value)
}

func (c FfiConverterActorObservation) LowerExternal(value ActorObservation) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ActorObservation](c, value))
}

func (c FfiConverterActorObservation) Write(writer io.Writer, value ActorObservation) {
		FfiConverterActorIdINSTANCE.Write(writer, value.ActorId);
		FfiConverterCodeSha256INSTANCE.Write(writer, value.CodeSha256);
		FfiConverterStringINSTANCE.Write(writer, value.HomeRegion);
		FfiConverterActorStateINSTANCE.Write(writer, value.State);
		FfiConverterSequenceSubscriptionObservationINSTANCE.Write(writer, value.Subscriptions);
		FfiConverterOptionalUint64INSTANCE.Write(writer, value.CheckpointUnixMillis);
		FfiConverterUint64INSTANCE.Write(writer, value.CheckpointEpoch);
		FfiConverterUint64INSTANCE.Write(writer, value.ConfigurationRevision);
}

type FfiDestroyerActorObservation struct {}

func (_ FfiDestroyerActorObservation) Destroy(value ActorObservation) {
	value.Destroy()
}

// Wire header projected from the canonical header alias.
type Header struct {
	Name string
	Value string
}

func (r *Header) Destroy() {
		FfiDestroyerString{}.Destroy(r.Name);
		FfiDestroyerString{}.Destroy(r.Value);
}

type FfiConverterHeader struct {}

var FfiConverterHeaderINSTANCE = FfiConverterHeader{}

func (c FfiConverterHeader) Lift(rb RustBufferI) Header {
	return LiftFromRustBuffer[Header](c, rb)
}

func (c FfiConverterHeader) Read(reader io.Reader) Header {
	return Header {
			FfiConverterStringINSTANCE.Read(reader),
			FfiConverterStringINSTANCE.Read(reader),
	}
}

func (c FfiConverterHeader) Lower(value Header) C.RustBuffer {
	return LowerIntoRustBuffer[Header](c, value)
}

func (c FfiConverterHeader) LowerExternal(value Header) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Header](c, value))
}

func (c FfiConverterHeader) Write(writer io.Writer, value Header) {
		FfiConverterStringINSTANCE.Write(writer, value.Name);
		FfiConverterStringINSTANCE.Write(writer, value.Value);
}

type FfiDestroyerHeader struct {}

func (_ FfiDestroyerHeader) Destroy(value Header) {
	value.Destroy()
}

// Typed invocation response with byte-preserving body and headers.
type InvokeActorResponse struct {
	Status uint32
	Body []byte
	Headers []Header
}

func (r *InvokeActorResponse) Destroy() {
		FfiDestroyerUint32{}.Destroy(r.Status);
		FfiDestroyerBytes{}.Destroy(r.Body);
		FfiDestroyerSequenceHeader{}.Destroy(r.Headers);
}

type FfiConverterInvokeActorResponse struct {}

var FfiConverterInvokeActorResponseINSTANCE = FfiConverterInvokeActorResponse{}

func (c FfiConverterInvokeActorResponse) Lift(rb RustBufferI) InvokeActorResponse {
	return LiftFromRustBuffer[InvokeActorResponse](c, rb)
}

func (c FfiConverterInvokeActorResponse) Read(reader io.Reader) InvokeActorResponse {
	return InvokeActorResponse {
			FfiConverterUint32INSTANCE.Read(reader),
			FfiConverterBytesINSTANCE.Read(reader),
			FfiConverterSequenceHeaderINSTANCE.Read(reader),
	}
}

func (c FfiConverterInvokeActorResponse) Lower(value InvokeActorResponse) C.RustBuffer {
	return LowerIntoRustBuffer[InvokeActorResponse](c, value)
}

func (c FfiConverterInvokeActorResponse) LowerExternal(value InvokeActorResponse) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[InvokeActorResponse](c, value))
}

func (c FfiConverterInvokeActorResponse) Write(writer io.Writer, value InvokeActorResponse) {
		FfiConverterUint32INSTANCE.Write(writer, value.Status);
		FfiConverterBytesINSTANCE.Write(writer, value.Body);
		FfiConverterSequenceHeaderINSTANCE.Write(writer, value.Headers);
}

type FfiDestroyerInvokeActorResponse struct {}

func (_ FfiDestroyerInvokeActorResponse) Destroy(value InvokeActorResponse) {
	value.Destroy()
}

// Strongly typed projection of one observed subscription.
type SubscriptionObservation struct {
	SubscriptionId string
	StreamPath string
	State SubscriptionState
	DeliveredCursor uint64
	CompletedCursor uint64
	RecoverableCursor uint64
	PlacementAnchor bool
	RetryCount uint32
	FailureCode string
	FailedCursor *uint64
}

func (r *SubscriptionObservation) Destroy() {
		FfiDestroyerString{}.Destroy(r.SubscriptionId);
		FfiDestroyerString{}.Destroy(r.StreamPath);
		FfiDestroyerSubscriptionState{}.Destroy(r.State);
		FfiDestroyerUint64{}.Destroy(r.DeliveredCursor);
		FfiDestroyerUint64{}.Destroy(r.CompletedCursor);
		FfiDestroyerUint64{}.Destroy(r.RecoverableCursor);
		FfiDestroyerBool{}.Destroy(r.PlacementAnchor);
		FfiDestroyerUint32{}.Destroy(r.RetryCount);
		FfiDestroyerString{}.Destroy(r.FailureCode);
		FfiDestroyerOptionalUint64{}.Destroy(r.FailedCursor);
}

type FfiConverterSubscriptionObservation struct {}

var FfiConverterSubscriptionObservationINSTANCE = FfiConverterSubscriptionObservation{}

func (c FfiConverterSubscriptionObservation) Lift(rb RustBufferI) SubscriptionObservation {
	return LiftFromRustBuffer[SubscriptionObservation](c, rb)
}

func (c FfiConverterSubscriptionObservation) Read(reader io.Reader) SubscriptionObservation {
	return SubscriptionObservation {
			FfiConverterStringINSTANCE.Read(reader),
			FfiConverterStringINSTANCE.Read(reader),
			FfiConverterSubscriptionStateINSTANCE.Read(reader),
			FfiConverterUint64INSTANCE.Read(reader),
			FfiConverterUint64INSTANCE.Read(reader),
			FfiConverterUint64INSTANCE.Read(reader),
			FfiConverterBoolINSTANCE.Read(reader),
			FfiConverterUint32INSTANCE.Read(reader),
			FfiConverterStringINSTANCE.Read(reader),
			FfiConverterOptionalUint64INSTANCE.Read(reader),
	}
}

func (c FfiConverterSubscriptionObservation) Lower(value SubscriptionObservation) C.RustBuffer {
	return LowerIntoRustBuffer[SubscriptionObservation](c, value)
}

func (c FfiConverterSubscriptionObservation) LowerExternal(value SubscriptionObservation) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SubscriptionObservation](c, value))
}

func (c FfiConverterSubscriptionObservation) Write(writer io.Writer, value SubscriptionObservation) {
		FfiConverterStringINSTANCE.Write(writer, value.SubscriptionId);
		FfiConverterStringINSTANCE.Write(writer, value.StreamPath);
		FfiConverterSubscriptionStateINSTANCE.Write(writer, value.State);
		FfiConverterUint64INSTANCE.Write(writer, value.DeliveredCursor);
		FfiConverterUint64INSTANCE.Write(writer, value.CompletedCursor);
		FfiConverterUint64INSTANCE.Write(writer, value.RecoverableCursor);
		FfiConverterBoolINSTANCE.Write(writer, value.PlacementAnchor);
		FfiConverterUint32INSTANCE.Write(writer, value.RetryCount);
		FfiConverterStringINSTANCE.Write(writer, value.FailureCode);
		FfiConverterOptionalUint64INSTANCE.Write(writer, value.FailedCursor);
}

type FfiDestroyerSubscriptionObservation struct {}

func (_ FfiDestroyerSubscriptionObservation) Destroy(value SubscriptionObservation) {
	value.Destroy()
}


// Known Actors state values emitted by the canonical Rust domain.
type ActorState uint

const (
	ActorStateUnspecified ActorState = 1
	ActorStateActive ActorState = 2
	ActorStateHibernated ActorState = 3
	ActorStatePaused ActorState = 4
)

type FfiConverterActorState struct {}

var FfiConverterActorStateINSTANCE = FfiConverterActorState{}

func (c FfiConverterActorState) Lift(rb RustBufferI) ActorState {
	return LiftFromRustBuffer[ActorState](c, rb)
}

func (c FfiConverterActorState) Lower(value ActorState) C.RustBuffer {
	return LowerIntoRustBuffer[ActorState](c, value)
}

func (c FfiConverterActorState) LowerExternal(value ActorState) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[ActorState](c, value))
}
func (FfiConverterActorState) Read(reader io.Reader) ActorState {
	id := readInt32(reader)
	return ActorState(id)
}

func (FfiConverterActorState) Write(writer io.Writer, value ActorState) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerActorState struct {}

func (_ FfiDestroyerActorState) Destroy(value ActorState) {
}

// Errors crossing the generated foreign-language boundary.
type BindingError struct {
	err error
}

// Convenience method to turn *BindingError into error
// Avoiding treating nil pointer as non nil error interface
func (err *BindingError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err BindingError) Error() string {
	return fmt.Sprintf("BindingError: %s", err.err.Error())
}

func (err BindingError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrBindingErrorConfiguration = fmt.Errorf("BindingErrorConfiguration")
var ErrBindingErrorTransport = fmt.Errorf("BindingErrorTransport")
var ErrBindingErrorService = fmt.Errorf("BindingErrorService")
var ErrBindingErrorContract = fmt.Errorf("BindingErrorContract")
var ErrBindingErrorSemantic = fmt.Errorf("BindingErrorSemantic")
var ErrBindingErrorCancelled = fmt.Errorf("BindingErrorCancelled")

// Variant structs
// Endpoint, credential, or trust configuration was rejected by Rust.
type BindingErrorConfiguration struct {
	DetailMessage string
}
// Endpoint, credential, or trust configuration was rejected by Rust.
func NewBindingErrorConfiguration(
	detailMessage string,
) *BindingError {
	return &BindingError { err: &BindingErrorConfiguration {
			DetailMessage: detailMessage,} }
}

func (e BindingErrorConfiguration) destroy() {
		FfiDestroyerString{}.Destroy(e.DetailMessage)
}


func (err BindingErrorConfiguration) Error() string {
	return fmt.Sprint("Configuration",
		": ",
		
		"DetailMessage=",
		err.DetailMessage,
	)
}

func (self BindingErrorConfiguration) Is(target error) bool {
	return target == ErrBindingErrorConfiguration
}
// The Rust-owned transport could not be constructed or reached.
type BindingErrorTransport struct {
	DetailMessage string
}
// The Rust-owned transport could not be constructed or reached.
func NewBindingErrorTransport(
	detailMessage string,
) *BindingError {
	return &BindingError { err: &BindingErrorTransport {
			DetailMessage: detailMessage,} }
}

func (e BindingErrorTransport) destroy() {
		FfiDestroyerString{}.Destroy(e.DetailMessage)
}


func (err BindingErrorTransport) Error() string {
	return fmt.Sprint("Transport",
		": ",
		
		"DetailMessage=",
		err.DetailMessage,
	)
}

func (self BindingErrorTransport) Is(target error) bool {
	return target == ErrBindingErrorTransport
}
// The service rejected an operation.
type BindingErrorService struct {
	GrpcCode int32
	ServiceCode *int32
	DetailMessage string
}
// The service rejected an operation.
func NewBindingErrorService(
	grpcCode int32,
	serviceCode *int32,
	detailMessage string,
) *BindingError {
	return &BindingError { err: &BindingErrorService {
			GrpcCode: grpcCode,
			ServiceCode: serviceCode,
			DetailMessage: detailMessage,} }
}

func (e BindingErrorService) destroy() {
		FfiDestroyerInt32{}.Destroy(e.GrpcCode)
		FfiDestroyerOptionalInt32{}.Destroy(e.ServiceCode)
		FfiDestroyerString{}.Destroy(e.DetailMessage)
}


func (err BindingErrorService) Error() string {
	return fmt.Sprint("Service",
		": ",
		
		"GrpcCode=",
		err.GrpcCode,
		", ",
		"ServiceCode=",
		err.ServiceCode,
		", ",
		"DetailMessage=",
		err.DetailMessage,
	)
}

func (self BindingErrorService) Is(target error) bool {
	return target == ErrBindingErrorService
}
// Rust's canonical request validator rejected an operation.
type BindingErrorContract struct {
	DetailMessage string
}
// Rust's canonical request validator rejected an operation.
func NewBindingErrorContract(
	detailMessage string,
) *BindingError {
	return &BindingError { err: &BindingErrorContract {
			DetailMessage: detailMessage,} }
}

func (e BindingErrorContract) destroy() {
		FfiDestroyerString{}.Destroy(e.DetailMessage)
}


func (err BindingErrorContract) Error() string {
	return fmt.Sprint("Contract",
		": ",
		
		"DetailMessage=",
		err.DetailMessage,
	)
}

func (self BindingErrorContract) Is(target error) bool {
	return target == ErrBindingErrorContract
}
// Rust could not project a wire value into its semantic domain.
type BindingErrorSemantic struct {
	DetailMessage string
}
// Rust could not project a wire value into its semantic domain.
func NewBindingErrorSemantic(
	detailMessage string,
) *BindingError {
	return &BindingError { err: &BindingErrorSemantic {
			DetailMessage: detailMessage,} }
}

func (e BindingErrorSemantic) destroy() {
		FfiDestroyerString{}.Destroy(e.DetailMessage)
}


func (err BindingErrorSemantic) Error() string {
	return fmt.Sprint("Semantic",
		": ",
		
		"DetailMessage=",
		err.DetailMessage,
	)
}

func (self BindingErrorSemantic) Is(target error) bool {
	return target == ErrBindingErrorSemantic
}
// The Rust-owned operation observed cancellation.
type BindingErrorCancelled struct {
}
// The Rust-owned operation observed cancellation.
func NewBindingErrorCancelled(
) *BindingError {
	return &BindingError { err: &BindingErrorCancelled {} }
}

func (e BindingErrorCancelled) destroy() {
}


func (err BindingErrorCancelled) Error() string {
	return fmt.Sprint("Cancelled",
		
	)
}

func (self BindingErrorCancelled) Is(target error) bool {
	return target == ErrBindingErrorCancelled
}

type FfiConverterBindingError struct{}

var FfiConverterBindingErrorINSTANCE = FfiConverterBindingError{}

func (c FfiConverterBindingError) Lift(eb RustBufferI) *BindingError {
	return LiftFromRustBuffer[*BindingError](c, eb)
}

func (c FfiConverterBindingError) Lower(value *BindingError) C.RustBuffer {
	return LowerIntoRustBuffer[*BindingError](c, value)
}

func (c FfiConverterBindingError) LowerExternal(value *BindingError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*BindingError](c, value))
}

func (c FfiConverterBindingError) Read(reader io.Reader) *BindingError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &BindingError{ &BindingErrorConfiguration{
			DetailMessage: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &BindingError{ &BindingErrorTransport{
			DetailMessage: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 3:
		return &BindingError{ &BindingErrorService{
			GrpcCode: FfiConverterInt32INSTANCE.Read(reader),
			ServiceCode: FfiConverterOptionalInt32INSTANCE.Read(reader),
			DetailMessage: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 4:
		return &BindingError{ &BindingErrorContract{
			DetailMessage: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 5:
		return &BindingError{ &BindingErrorSemantic{
			DetailMessage: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 6:
		return &BindingError{ &BindingErrorCancelled{
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterBindingError.Read()", errorID))
	}
}

func (c FfiConverterBindingError) Write(writer io.Writer, value *BindingError) {
	switch variantValue := value.err.(type) {
		case *BindingErrorConfiguration:
			writeInt32(writer, 1)
			FfiConverterStringINSTANCE.Write(writer, variantValue.DetailMessage)
		case *BindingErrorTransport:
			writeInt32(writer, 2)
			FfiConverterStringINSTANCE.Write(writer, variantValue.DetailMessage)
		case *BindingErrorService:
			writeInt32(writer, 3)
			FfiConverterInt32INSTANCE.Write(writer, variantValue.GrpcCode)
			FfiConverterOptionalInt32INSTANCE.Write(writer, variantValue.ServiceCode)
			FfiConverterStringINSTANCE.Write(writer, variantValue.DetailMessage)
		case *BindingErrorContract:
			writeInt32(writer, 4)
			FfiConverterStringINSTANCE.Write(writer, variantValue.DetailMessage)
		case *BindingErrorSemantic:
			writeInt32(writer, 5)
			FfiConverterStringINSTANCE.Write(writer, variantValue.DetailMessage)
		case *BindingErrorCancelled:
			writeInt32(writer, 6)
		default:
			_ = variantValue
			panic(fmt.Sprintf("invalid error value `%v` in FfiConverterBindingError.Write", value))
	}
}

type FfiDestroyerBindingError struct {}

func (_ FfiDestroyerBindingError) Destroy(value *BindingError) {
	switch variantValue := value.err.(type) {
		case BindingErrorConfiguration:
			variantValue.destroy()
		case BindingErrorTransport:
			variantValue.destroy()
		case BindingErrorService:
			variantValue.destroy()
		case BindingErrorContract:
			variantValue.destroy()
		case BindingErrorSemantic:
			variantValue.destroy()
		case BindingErrorCancelled:
			variantValue.destroy()
		default:
			_ = variantValue
			panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerBindingError.Destroy", value))
	}
}



// Rust-owned subscription start selector, preserving cursor zero and
// current-head presence semantics.
type SubscriptionStart interface {
	Destroy()
}
type SubscriptionStartCursor struct {
	Cursor uint64
}

func (e SubscriptionStartCursor) Destroy() {
		FfiDestroyerUint64{}.Destroy(e.Cursor);
}
type SubscriptionStartCurrentHead struct {
	CurrentHead bool
}

func (e SubscriptionStartCurrentHead) Destroy() {
		FfiDestroyerBool{}.Destroy(e.CurrentHead);
}

type FfiConverterSubscriptionStart struct {}

var FfiConverterSubscriptionStartINSTANCE = FfiConverterSubscriptionStart{}

func (c FfiConverterSubscriptionStart) Lift(rb RustBufferI) SubscriptionStart {
	return LiftFromRustBuffer[SubscriptionStart](c, rb)
}

func (c FfiConverterSubscriptionStart) Lower(value SubscriptionStart) C.RustBuffer {
	return LowerIntoRustBuffer[SubscriptionStart](c, value)
}

func (c FfiConverterSubscriptionStart) LowerExternal(value SubscriptionStart) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SubscriptionStart](c, value))
}
func (FfiConverterSubscriptionStart) Read(reader io.Reader) SubscriptionStart {
	id := readInt32(reader)
	switch (id) {
		case 1:
			return SubscriptionStartCursor{
				FfiConverterUint64INSTANCE.Read(reader),
			};
		case 2:
			return SubscriptionStartCurrentHead{
				FfiConverterBoolINSTANCE.Read(reader),
			};
		default:
			panic(fmt.Sprintf("invalid enum value %v in FfiConverterSubscriptionStart.Read()", id));
	}
}

func (FfiConverterSubscriptionStart) Write(writer io.Writer, value SubscriptionStart) {
	switch variant_value := value.(type) {
		case SubscriptionStartCursor:
			writeInt32(writer, 1)
			FfiConverterUint64INSTANCE.Write(writer, variant_value.Cursor)
		case SubscriptionStartCurrentHead:
			writeInt32(writer, 2)
			FfiConverterBoolINSTANCE.Write(writer, variant_value.CurrentHead)
		default:
			_ = variant_value
			panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterSubscriptionStart.Write", value))
	}
}

type FfiDestroyerSubscriptionStart struct {}

func (_ FfiDestroyerSubscriptionStart) Destroy(value SubscriptionStart) {
	value.Destroy()
}



// Known subscription state values emitted by the canonical Rust domain.
type SubscriptionState uint

const (
	SubscriptionStateUnspecified SubscriptionState = 1
	SubscriptionStateActive SubscriptionState = 2
	SubscriptionStatePaused SubscriptionState = 3
)

type FfiConverterSubscriptionState struct {}

var FfiConverterSubscriptionStateINSTANCE = FfiConverterSubscriptionState{}

func (c FfiConverterSubscriptionState) Lift(rb RustBufferI) SubscriptionState {
	return LiftFromRustBuffer[SubscriptionState](c, rb)
}

func (c FfiConverterSubscriptionState) Lower(value SubscriptionState) C.RustBuffer {
	return LowerIntoRustBuffer[SubscriptionState](c, value)
}

func (c FfiConverterSubscriptionState) LowerExternal(value SubscriptionState) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[SubscriptionState](c, value))
}
func (FfiConverterSubscriptionState) Read(reader io.Reader) SubscriptionState {
	id := readInt32(reader)
	return SubscriptionState(id)
}

func (FfiConverterSubscriptionState) Write(writer io.Writer, value SubscriptionState) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerSubscriptionState struct {}

func (_ FfiDestroyerSubscriptionState) Destroy(value SubscriptionState) {
}



type FfiConverterOptionalInt32 struct{}

var FfiConverterOptionalInt32INSTANCE = FfiConverterOptionalInt32{}

func (c FfiConverterOptionalInt32) Lift(rb RustBufferI) *int32 {
	return LiftFromRustBuffer[*int32](c, rb)
}

func (_ FfiConverterOptionalInt32) Read(reader io.Reader) *int32 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterInt32INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalInt32) Lower(value *int32) C.RustBuffer {
	return LowerIntoRustBuffer[*int32](c, value)
}

func (c FfiConverterOptionalInt32) LowerExternal(value *int32) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*int32](c, value))
}

func (_ FfiConverterOptionalInt32) Write(writer io.Writer, value *int32) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterInt32INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalInt32 struct {}

func (_ FfiDestroyerOptionalInt32) Destroy(value *int32) {
	if value != nil {
		FfiDestroyerInt32{}.Destroy(*value)
	}
}


type FfiConverterOptionalUint64 struct{}

var FfiConverterOptionalUint64INSTANCE = FfiConverterOptionalUint64{}

func (c FfiConverterOptionalUint64) Lift(rb RustBufferI) *uint64 {
	return LiftFromRustBuffer[*uint64](c, rb)
}

func (_ FfiConverterOptionalUint64) Read(reader io.Reader) *uint64 {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterUint64INSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalUint64) Lower(value *uint64) C.RustBuffer {
	return LowerIntoRustBuffer[*uint64](c, value)
}

func (c FfiConverterOptionalUint64) LowerExternal(value *uint64) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*uint64](c, value))
}

func (_ FfiConverterOptionalUint64) Write(writer io.Writer, value *uint64) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterUint64INSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalUint64 struct {}

func (_ FfiDestroyerOptionalUint64) Destroy(value *uint64) {
	if value != nil {
		FfiDestroyerUint64{}.Destroy(*value)
	}
}


type FfiConverterOptionalBytes struct{}

var FfiConverterOptionalBytesINSTANCE = FfiConverterOptionalBytes{}

func (c FfiConverterOptionalBytes) Lift(rb RustBufferI) *[]byte {
	return LiftFromRustBuffer[*[]byte](c, rb)
}

func (_ FfiConverterOptionalBytes) Read(reader io.Reader) *[]byte {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterBytesINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalBytes) Lower(value *[]byte) C.RustBuffer {
	return LowerIntoRustBuffer[*[]byte](c, value)
}

func (c FfiConverterOptionalBytes) LowerExternal(value *[]byte) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*[]byte](c, value))
}

func (_ FfiConverterOptionalBytes) Write(writer io.Writer, value *[]byte) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterBytesINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalBytes struct {}

func (_ FfiDestroyerOptionalBytes) Destroy(value *[]byte) {
	if value != nil {
		FfiDestroyerBytes{}.Destroy(*value)
	}
}


type FfiConverterOptionalCancellationHandle struct{}

var FfiConverterOptionalCancellationHandleINSTANCE = FfiConverterOptionalCancellationHandle{}

func (c FfiConverterOptionalCancellationHandle) Lift(rb RustBufferI) **CancellationHandle {
	return LiftFromRustBuffer[**CancellationHandle](c, rb)
}

func (_ FfiConverterOptionalCancellationHandle) Read(reader io.Reader) **CancellationHandle {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterCancellationHandleINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalCancellationHandle) Lower(value **CancellationHandle) C.RustBuffer {
	return LowerIntoRustBuffer[**CancellationHandle](c, value)
}

func (c FfiConverterOptionalCancellationHandle) LowerExternal(value **CancellationHandle) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[**CancellationHandle](c, value))
}

func (_ FfiConverterOptionalCancellationHandle) Write(writer io.Writer, value **CancellationHandle) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterCancellationHandleINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalCancellationHandle struct {}

func (_ FfiDestroyerOptionalCancellationHandle) Destroy(value **CancellationHandle) {
	if value != nil {
		FfiDestroyerCancellationHandle{}.Destroy(*value)
	}
}


type FfiConverterOptionalActorObservation struct{}

var FfiConverterOptionalActorObservationINSTANCE = FfiConverterOptionalActorObservation{}

func (c FfiConverterOptionalActorObservation) Lift(rb RustBufferI) *ActorObservation {
	return LiftFromRustBuffer[*ActorObservation](c, rb)
}

func (_ FfiConverterOptionalActorObservation) Read(reader io.Reader) *ActorObservation {
	if readInt8(reader) == 0 {
		return nil
	}
	temp := FfiConverterActorObservationINSTANCE.Read(reader)
	return &temp
}

func (c FfiConverterOptionalActorObservation) Lower(value *ActorObservation) C.RustBuffer {
	return LowerIntoRustBuffer[*ActorObservation](c, value)
}

func (c FfiConverterOptionalActorObservation) LowerExternal(value *ActorObservation) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*ActorObservation](c, value))
}

func (_ FfiConverterOptionalActorObservation) Write(writer io.Writer, value *ActorObservation) {
	if value == nil {
		writeInt8(writer, 0)
	} else {
		writeInt8(writer, 1)
		FfiConverterActorObservationINSTANCE.Write(writer, *value)
	}
}

type FfiDestroyerOptionalActorObservation struct {}

func (_ FfiDestroyerOptionalActorObservation) Destroy(value *ActorObservation) {
	if value != nil {
		FfiDestroyerActorObservation{}.Destroy(*value)
	}
}


type FfiConverterSequenceBinding struct{}

var FfiConverterSequenceBindingINSTANCE = FfiConverterSequenceBinding{}

func (c FfiConverterSequenceBinding) Lift(rb RustBufferI) []*Binding {
	return LiftFromRustBuffer[[]*Binding](c, rb)
}

func (c FfiConverterSequenceBinding) Read(reader io.Reader) []*Binding {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]*Binding, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterBindingINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceBinding) Lower(value []*Binding) C.RustBuffer {
	return LowerIntoRustBuffer[[]*Binding](c, value)
}

func (c FfiConverterSequenceBinding) LowerExternal(value []*Binding) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]*Binding](c, value))
}

func (c FfiConverterSequenceBinding) Write(writer io.Writer, value []*Binding) {
	if len(value) > math.MaxInt32 {
		panic("[]*Binding is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterBindingINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceBinding struct {}

func (FfiDestroyerSequenceBinding) Destroy(sequence []*Binding) {
	for _, value := range sequence {
		FfiDestroyerBinding{}.Destroy(value)	
	}
}


type FfiConverterSequenceSubscriptionSpec struct{}

var FfiConverterSequenceSubscriptionSpecINSTANCE = FfiConverterSequenceSubscriptionSpec{}

func (c FfiConverterSequenceSubscriptionSpec) Lift(rb RustBufferI) []*SubscriptionSpec {
	return LiftFromRustBuffer[[]*SubscriptionSpec](c, rb)
}

func (c FfiConverterSequenceSubscriptionSpec) Read(reader io.Reader) []*SubscriptionSpec {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]*SubscriptionSpec, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterSubscriptionSpecINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceSubscriptionSpec) Lower(value []*SubscriptionSpec) C.RustBuffer {
	return LowerIntoRustBuffer[[]*SubscriptionSpec](c, value)
}

func (c FfiConverterSequenceSubscriptionSpec) LowerExternal(value []*SubscriptionSpec) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]*SubscriptionSpec](c, value))
}

func (c FfiConverterSequenceSubscriptionSpec) Write(writer io.Writer, value []*SubscriptionSpec) {
	if len(value) > math.MaxInt32 {
		panic("[]*SubscriptionSpec is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterSubscriptionSpecINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceSubscriptionSpec struct {}

func (FfiDestroyerSequenceSubscriptionSpec) Destroy(sequence []*SubscriptionSpec) {
	for _, value := range sequence {
		FfiDestroyerSubscriptionSpec{}.Destroy(value)	
	}
}


type FfiConverterSequenceHeader struct{}

var FfiConverterSequenceHeaderINSTANCE = FfiConverterSequenceHeader{}

func (c FfiConverterSequenceHeader) Lift(rb RustBufferI) []Header {
	return LiftFromRustBuffer[[]Header](c, rb)
}

func (c FfiConverterSequenceHeader) Read(reader io.Reader) []Header {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]Header, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterHeaderINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceHeader) Lower(value []Header) C.RustBuffer {
	return LowerIntoRustBuffer[[]Header](c, value)
}

func (c FfiConverterSequenceHeader) LowerExternal(value []Header) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]Header](c, value))
}

func (c FfiConverterSequenceHeader) Write(writer io.Writer, value []Header) {
	if len(value) > math.MaxInt32 {
		panic("[]Header is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterHeaderINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceHeader struct {}

func (FfiDestroyerSequenceHeader) Destroy(sequence []Header) {
	for _, value := range sequence {
		FfiDestroyerHeader{}.Destroy(value)	
	}
}


type FfiConverterSequenceSubscriptionObservation struct{}

var FfiConverterSequenceSubscriptionObservationINSTANCE = FfiConverterSequenceSubscriptionObservation{}

func (c FfiConverterSequenceSubscriptionObservation) Lift(rb RustBufferI) []SubscriptionObservation {
	return LiftFromRustBuffer[[]SubscriptionObservation](c, rb)
}

func (c FfiConverterSequenceSubscriptionObservation) Read(reader io.Reader) []SubscriptionObservation {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([]SubscriptionObservation, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterSubscriptionObservationINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceSubscriptionObservation) Lower(value []SubscriptionObservation) C.RustBuffer {
	return LowerIntoRustBuffer[[]SubscriptionObservation](c, value)
}

func (c FfiConverterSequenceSubscriptionObservation) LowerExternal(value []SubscriptionObservation) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[]SubscriptionObservation](c, value))
}

func (c FfiConverterSequenceSubscriptionObservation) Write(writer io.Writer, value []SubscriptionObservation) {
	if len(value) > math.MaxInt32 {
		panic("[]SubscriptionObservation is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterSubscriptionObservationINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceSubscriptionObservation struct {}

func (FfiDestroyerSequenceSubscriptionObservation) Destroy(sequence []SubscriptionObservation) {
	for _, value := range sequence {
		FfiDestroyerSubscriptionObservation{}.Destroy(value)	
	}
}


const (
	uniffiRustFuturePollReady      int8 = 0
	uniffiRustFuturePollMaybeReady int8 = 1
)

type rustFuturePollFunc func(C.uint64_t, C.UniffiRustFutureContinuationCallback, C.uint64_t)
type rustFutureCompleteFunc[T any] func(C.uint64_t, *C.RustCallStatus) T
type rustFutureFreeFunc func(C.uint64_t)

//export acyclic_actors_uniffi_uniffiFutureContinuationCallback
func acyclic_actors_uniffi_uniffiFutureContinuationCallback(data C.uint64_t, pollResult C.int8_t) {
	h := cgo.Handle(uintptr(data))
	waiter := h.Value().(chan int8)
	waiter <- int8(pollResult)
}

func uniffiRustCallAsyncContext[E any, T any, F any](
	ctx context.Context,
	cancelFunc func(C.uint64_t),
	errConverter BufReader[E],
	completeFunc rustFutureCompleteFunc[F],
	liftFunc func(F) T,
	rustFuture C.uint64_t,
	pollFunc rustFuturePollFunc,
	freeFunc rustFutureFreeFunc,
) (T, E) {
	defer freeFunc(rustFuture)
	if ctx == nil { ctx = context.Background() }

	pollResult := int8(-1)
	waiter := make(chan int8, 1)

	chanHandle := cgo.NewHandle(waiter)
	defer chanHandle.Delete()

	for pollResult != uniffiRustFuturePollReady {
		pollFunc(
			rustFuture,
			(C.UniffiRustFutureContinuationCallback)(C.acyclic_actors_uniffi_uniffiFutureContinuationCallback),
			C.uint64_t(chanHandle),
		)
		select {
		case pollResult = <-waiter:
		case <-ctx.Done():
			cancelFunc(rustFuture)
			pollResult = <-waiter
		}
	}

	var goValue T
	ffiValue, err := rustCallWithError(errConverter, func(status *C.RustCallStatus) F {
		return completeFunc(rustFuture, status)
	})
	if value := reflect.ValueOf(err); value.IsValid() && !value.IsZero() {
		return goValue, err
	}
	return liftFunc(ffiValue), err
}

//export acyclic_actors_uniffi_uniffiFreeGorutine
func acyclic_actors_uniffi_uniffiFreeGorutine(data C.uint64_t) {
	handle := cgo.Handle(uintptr(data))
	defer handle.Delete()

	guard := handle.Value().(chan struct{})
	guard <- struct{}{}
}

// Connects through the canonical Rust transport and returns an opaque client.
func ConnectActors(endpoint string, token string, cancellation **CancellationHandle, ctx context.Context) (*ActorsClient, error) {
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_u64(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *ActorsClient {
			return FfiConverterActorsClientINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_func_connect_actors(FfiConverterStringINSTANCE.Lower(endpoint), FfiConverterStringINSTANCE.Lower(token), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}

// Connects with an optional caller-pinned CA certificate through the
// canonical Rust transport and returns an opaque client.
func ConnectActorsWithCa(endpoint string, token string, caCertificate *[]byte, cancellation **CancellationHandle, ctx context.Context) (*ActorsClient, error) {
	 res, err :=uniffiRustCallAsyncContext[*BindingError](
		ctx,
		func(handle C.uint64_t) { C.ffi_acyclic_actors_uniffi_rust_future_cancel_u64(handle) },
        FfiConverterBindingErrorINSTANCE,
		// completeFn
		func(handle C.uint64_t, status *C.RustCallStatus) C.uint64_t {
			res := C.ffi_acyclic_actors_uniffi_rust_future_complete_u64(handle, status)
			return res
		},
		// liftFn
		func(ffi C.uint64_t) *ActorsClient {
			return FfiConverterActorsClientINSTANCE.Lift(ffi)
		},
		C.uniffi_acyclic_actors_uniffi_fn_func_connect_actors_with_ca(FfiConverterStringINSTANCE.Lower(endpoint), FfiConverterStringINSTANCE.Lower(token), FfiConverterOptionalBytesINSTANCE.Lower(caCertificate), FfiConverterOptionalCancellationHandleINSTANCE.Lower(cancellation)),
		// pollFn
		func (handle C.uint64_t, continuation C.UniffiRustFutureContinuationCallback, data C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_poll_u64(handle, continuation, data)
		},
		// freeFn
		func (handle C.uint64_t) {
			C.ffi_acyclic_actors_uniffi_rust_future_free_u64(handle)
		},
	)

	if err == nil {
		return res, nil
	}

	return res, err 
}


