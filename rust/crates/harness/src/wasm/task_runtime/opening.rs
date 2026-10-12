use super::*;

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Opens existing providers with caller-supplied limits and signed authority.
    pub async fn open(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserTaskOptions")] options: JsValue,
        owner: &WasmReducer,
        registry: &WasmTaskRegistry,
        #[wasm_bindgen(unchecked_param_type = "Scope")] signed_scope: JsValue,
    ) -> std::result::Result<WasmTaskRuntime, JsValue> {
        let options: WasmBrowserTaskOptions = from_js(options)?;
        let scope: Scope = from_js(signed_scope)?;
        let verifier = owner.issuer.verifier(&mut None).expect("verifier copy");
        ContentGrant::verify(
            &verifier,
            &scope,
            &options.volume,
            VolumeOperation::Write,
            options.maximum_payload_bytes,
            &mut None,
        )
        .map_err(js_error)?;
        let root = RuntimeScope::new(scope.capabilities().clone(), options.limits)
            .map_err(js_error)?
            .with_run_limits(options.run_limits, &mut None)
            .map_err(js_error)?;
        let authority = IndexedDbAuthorityStore::open(
            &options.filesystem_database,
            options.maximum_object_bytes,
        )
        .await
        .map_err(JsValue::from)?;
        let objects =
            IndexedDbObjectStore::open(&options.filesystem_database, options.maximum_object_bytes)
                .await
                .map_err(JsValue::from)?;
        let filesystem = FilesystemHost::new(
            Fs::new(authority, objects, EmbeddedCapabilities { durable: true }),
            &options.filesystem_provider,
            None,
        )
        .map_err(js_error)?;
        let limits = options.stream_limits;
        let stream = Arc::new(
            BrowserStream::open(
                &options.stream_database,
                BrowserStreamLimits {
                    commands: limits.commands,
                    journal_bytes: limits.journal_bytes,
                    memory: MemoryLimits {
                        paths: limits.paths as usize,
                        path_bytes: limits.path_bytes as usize,
                        records: limits.records as usize,
                        payload_bytes: limits.payload_bytes as usize,
                        commits: limits.commits as usize,
                        idempotency_results: limits.idempotency_results as usize,
                    },
                },
            )
            .await
            .map_err(|error| js_error(Error::Storage(error.to_string())))?,
        );
        let source = options
            .recursive_tools
            .map(|config| {
                let graph_root = config.project_policy_graph_root;
                let source = default_tools::BrowserToolSource::new(
                    config,
                    stream.clone(),
                    filesystem.clone(),
                    &options.filesystem_provider,
                    root.limits(),
                )?;
                if let Some(graph_root) = graph_root {
                    source.with_project_source_policy(
                        owner.issuer.clone(),
                        Arc::new(crate::filesystem::FsRootProjectSourceProvider::new(
                            filesystem.clone(),
                            &owner.issuer.audience(),
                            graph_root,
                            None,
                        )?),
                    )?;
                }
                Ok::<_, Error>(source)
            })
            .transpose()
            .map_err(js_error)?;
        let defaults = crate::filesystem::recursive_default_tools(
            source.clone().map(|source| {
                source
                    as Arc<
                        dyn crate::filesystem::RecursiveDefaultToolSource<
                                BrowserStream,
                                IndexedDbAuthorityStore,
                                IndexedDbObjectStore,
                            >,
                    >
            }),
            options.maximum_payload_bytes,
        )
        .map_err(js_error)?;
        let mut tools = registry.tools.clone();
        let mut resumable_tools = crate::durable_tool::ResumableToolRegistry::default();
        for definition in defaults.tools.definitions(None).map_err(js_error)? {
            if tools
                .get(&definition.name, None)
                .map_err(js_error)?
                .is_none()
            {
                tools
                    .register(
                        (
                            defaults
                                .tools
                                .get(&definition.name, None)
                                .map_err(js_error)?
                                .expect("registered default")
                                .clone(),
                            None,
                        ),
                        &mut None,
                        None,
                    )
                    .map_err(js_error)?;
                if let Some(tool) = defaults
                    .resumable_tools
                    .get(&definition.name, &definition.revision, &mut None)
                    .map_err(js_error)?
                {
                    resumable_tools.register(tool.clone()).map_err(js_error)?;
                }
            }
        }
        let runtime = FilesystemTaskRuntime::open(
            stream.clone(),
            filesystem.clone(),
            &options.volume,
            &verifier,
            &scope,
            &root,
            &registry.tasks,
            &registry.machines,
            &tools,
            options.session_limits,
            options.concurrency as usize,
            options.maximum_payload_bytes,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
            &[Arc::new(
                crate::filesystem::FilesystemResourceCreationProvider::new(
                    filesystem.clone(),
                    &owner.issuer,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                )
                .map_err(js_error)?,
            )],
            &resumable_tools,
            None,
        )
        .await
        .map_err(js_error)?;
        let runtime = Arc::new(match &source {
            Some(source) => runtime.with_recursive_source(source.clone()),
            None => runtime,
        });
        if let Some(source) = source {
            source.bind_runtime(&runtime).map_err(js_error)?;
        }
        Ok(Self {
            runtime,
            stream,
            filesystem,
            volume: options.volume,
            scope,
            verifier,
            maximum_payload_bytes: options.maximum_payload_bytes,
            model: None,
        })
    }

    /// Composes an explicit provider with the stock model command executor.
    #[wasm_bindgen(js_name = configureModel)]
    pub fn configure_model(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Model")] model: JsValue,
        #[wasm_bindgen(
            unchecked_param_type = "(request: Uint8Array, dispatch: ModelDispatch, signal: AbortSignal) => AsyncIterator<ModelEvent>"
        )]
        generate: Function,
        #[wasm_bindgen(
            unchecked_param_type = "(attempt: ModelDispatch & { observed: ModelEvent[] }) => ModelEvent[] | null | Promise<ModelEvent[] | null>"
        )]
        reconcile: Function,
        #[wasm_bindgen(
            unchecked_param_type = "((model: Model) => ModelContextCapacity) | undefined"
        )]
        capacity: Option<Function>,
        #[wasm_bindgen(
            unchecked_param_type = "((request: Uint8Array, request_digest: number[]) => ModelTokenCount) | undefined"
        )]
        count_tokens: Option<Function>,
        #[wasm_bindgen(unchecked_param_type = "CompactionPolicy")] compaction: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let model: Model = from_js(model)?;
        model.validate().map_err(js_error)?;
        let compaction = from_js(compaction)?;
        self.model = Some((
            model,
            Arc::new(HostModel {
                generate,
                reconcile,
                capacity,
                count_tokens,
            }),
            compaction,
        ));
        Ok(())
    }

    /// Creates only the explicitly authorized private journal volume.
    #[wasm_bindgen(js_name = initializeVolume)]
    pub async fn initialize_volume(&self) -> std::result::Result<(), JsValue> {
        ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Write,
            self.maximum_payload_bytes,
            &mut None,
        )
        .map_err(js_error)?;
        self.filesystem
            .create_volume(&self.volume)
            .await
            .map_err(js_error)?;
        Ok(())
    }
}
