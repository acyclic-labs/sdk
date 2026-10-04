# frozen_string_literal: true

require "json"
require "minitest/autorun"
require "acyclic_sdk"

class RustFamilyGoldensTest < Minitest::Test
  GOLDENS = File.expand_path(
    ENV.fetch(
      "RUST_FAMILY_GOLDENS",
      "fixtures/rust-family-goldens.json"
    ),
    __dir__
  )

  def test_all_nine_rust_family_goldens_round_trip_exactly
    fixtures = JSON.parse(File.read(GOLDENS))
    assert_equal 9, fixtures.length
    assert_equal 9, fixtures.map { |fixture| fixture.fetch("family") }.uniq.length

    fixtures.each do |fixture|
      klass = constant_for(fixture.fetch("message"))
      wire = [fixture.fetch("wire_hex")].pack("H*")
      message = klass.decode(wire)

      assert_equal wire, klass.encode(message), fixture.fetch("family")
      assert_equal fixture.fetch("json"), message.to_json, fixture.fetch("family")

      decoded = JSON.parse(message.to_json)
      assert_equal fixture.fetch("value"), decoded.fetch(fixture.fetch("field")).to_s,
                   fixture.fetch("family")
    end
  end

  private

  def constant_for(message_name)
    segments = message_name.split(".")
    namespace = segments[0...-1].reduce(Object) do |current, segment|
      current.const_get(segment.split("_").map(&:capitalize).join)
    end
    namespace.const_get(segments.fetch(-1))
  end
end
