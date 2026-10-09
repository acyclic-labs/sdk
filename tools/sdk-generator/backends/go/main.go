package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	sourceRoot := flag.String("source-root", "", "source checkout root")
	authorityRoot := flag.String("authority", "", "staged Rust authority directory or manifest")
	requestPath := flag.String("request", "", "generation request JSON")
	outputRoot := flag.String("output", "", "isolated package output")
	protoc := flag.String("protoc", "protoc", "pinned protoc executable")
	genGo := flag.String("protoc-gen-go", "protoc-gen-go", "pinned Go protobuf plugin")
	genGRPC := flag.String("protoc-gen-go-grpc", "protoc-gen-go-grpc", "pinned Go gRPC plugin")
	wantGo := flag.String("go-version", "", "required Go version")
	wantProtoc := flag.String("protoc-version", "", "required protoc version")
	wantGenGo := flag.String("protoc-gen-go-version", "", "required protoc-gen-go version")
	wantGenGRPC := flag.String("protoc-gen-go-grpc-version", "", "required protoc-gen-go-grpc version")
	flag.Parse()
	if err := run(*sourceRoot, *authorityRoot, *requestPath, *outputRoot, *protoc, *genGo, *genGRPC, *wantGo, *wantProtoc, *wantGenGo, *wantGenGRPC); err != nil {
		fmt.Fprintln(os.Stderr, "sdk-go-producer:", err)
		os.Exit(1)
	}
}
