#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "api/DefaultAPI.h"
#include "model/acyclic_workers_v1_invoke_deployment_request.h"

int main(void) {
    const char *port = getenv("PORT");
    const char *expected_revision = getenv("REV");
    const char *expect_error = getenv("EXPECT_ERROR");
    char base_path[64];
    snprintf(base_path, sizeof(base_path), "http://127.0.0.1:%s", port ? port : "18765");
    apiClient_setupGlobalEnv();
    apiClient_t *client = apiClient_create_with_base_path(base_path, NULL);
    if (!client) return 2;
    acyclic_workers_v1_invoke_deployment_request_t *request = acyclic_workers_v1_invoke_deployment_request_create(
        "prod", "AQID", NULL, "POST", "/hello");
    acyclic_workers_v1_invoke_response_t *response = DefaultAPI_invokeDeployment(client, "prod", request);
    fprintf(stderr, "after_api status=%ld response=%p\n", client->response_code, (void *)response);
    fflush(stderr);
    if (expect_error && strcmp(expect_error, "1") == 0) {
        if (response != NULL || client->response_code != 409) return 4;
        printf("error_status=%ld\n", client->response_code);
        return 0;
    }
    if (!response || client->response_code != 200 || strcmp(response->body, "b2s=") != 0 ||
        strcmp(response->resolved_sha256, "AQID") != 0 || strcmp(response->resolved_revision, expected_revision ? expected_revision : "1") != 0) {
        fprintf(stderr, "unexpected status=%ld response=%p\n", client->response_code, (void *)response);
        return 3;
    }
    printf("status=%ld body=%s resolvedSha256=%s resolvedRevision=%s\n", client->response_code,
           response->body, response->resolved_sha256, response->resolved_revision);
    /* Process cleanup is sufficient for this bounded consumer; generated free paths are unchanged. */
    return 0;
}
