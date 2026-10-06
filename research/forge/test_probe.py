import copy
import unittest

from probe import analyze


def _document() -> dict:
    return {
        "openapi": "3.0.3",
        "x-acyclic-source": {"descriptor_sha256": "abc123"},
        "paths": {
            "/v1/actors/inspect": {
                "post": {
                    "operationId": "inspectActor",
                    "description": "Inspect one actor.",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/InspectRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/InspectResponse"}
                                }
                            }
                        }
                    },
                    "x-protobuf-rpc": "acyclic.actors.v1.ActorsService/InspectActor",
                }
            }
        },
        "components": {
            "schemas": {
                "InspectRequest": {
                    "type": "object",
                    "properties": {"actorId": {"type": "string"}},
                    "x-protobuf-presence": "implicit",
                },
                "InspectResponse": {"type": "object"},
            }
        },
    }


class ProbeTests(unittest.TestCase):
    def test_valid_rust_projection_reports_semantic_coverage(self):
        report = analyze(_document())
        self.assertTrue(report["valid"])
        self.assertEqual(report["summary"]["operation_count"], 1)
        self.assertEqual(report["summary"]["description_coverage"], 1.0)
        self.assertEqual(report["summary"]["operations_with_rpc_identity"], 1)
        self.assertEqual(report["summary"]["semantic_extension_counts"]["x-protobuf-presence"], 1)

    def test_empty_operation_description_is_a_docs_failure(self):
        document = copy.deepcopy(_document())
        document["paths"]["/v1/actors/inspect"]["post"]["description"] = "  "
        report = analyze(document)
        self.assertFalse(report["valid"])
        self.assertIn("missing-operation-description", {issue["code"] for issue in report["issues"]})
        self.assertEqual(report["summary"]["description_coverage"], 0.0)

    def test_bad_schema_reference_is_a_semantic_failure(self):
        document = copy.deepcopy(_document())
        schema = document["paths"]["/v1/actors/inspect"]["post"]["requestBody"]["content"]["application/json"]["schema"]
        schema["$ref"] = "#/components/schemas/MissingRequest"
        report = analyze(document)
        self.assertFalse(report["valid"])
        self.assertIn("unresolved-schema-ref", {issue["code"] for issue in report["issues"]})

    def test_missing_rpc_identity_is_not_silently_accepted(self):
        document = copy.deepcopy(_document())
        del document["paths"]["/v1/actors/inspect"]["post"]["x-protobuf-rpc"]
        report = analyze(document)
        self.assertFalse(report["valid"])
        self.assertIn("missing-rpc-identity", {issue["code"] for issue in report["issues"]})


if __name__ == "__main__":
    unittest.main()
