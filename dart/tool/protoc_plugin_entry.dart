import 'dart:io';

import 'package:protoc_plugin/protoc.dart';

void main() {
  CodeGenerator(stdin, stdout).generate();
}
