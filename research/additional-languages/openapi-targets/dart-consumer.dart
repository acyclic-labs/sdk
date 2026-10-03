import 'dart:io';

import 'package:openapi/api.dart';

Future<void> main() async {
  final port = Platform.environment['PORT'] ?? '18765';
  final revision = Platform.environment['REV'] ?? '1';
  final api = DefaultApi(ApiClient(basePath: 'http://127.0.0.1:$port'));
  try {
    final response = await api.invokeDeployment(
      'prod',
      AcyclicWorkersV1InvokeDeploymentRequest(
        alias: 'prod',
        body: 'AQID',
        method: 'POST',
        url: '/hello',
      ),
    );
    if (response == null || response.status != 200 || response.body != 'b2s=' ||
        response.resolvedSha256 != 'AQID' || response.resolvedRevision != revision) {
      throw StateError('unexpected response: $response');
    }
    print('status=${response.status} body=${response.body} resolvedSha256=${response.resolvedSha256} resolvedRevision=${response.resolvedRevision}');
  } on ApiException catch (error) {
    if (error.code != 409 || !(error.message ?? '').contains('fixture rejection')) {
      rethrow;
    }
    print('error=${error.code} message=${error.message}');
  }
}
