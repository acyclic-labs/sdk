use super::*;

#[wasm_bindgen]
impl WasmTaskRegistry {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Callbacks share one immutable definition revision and are trusted host implementations.
    #[wasm_bindgen(js_name = registerTool)]
    pub fn register_tool(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ToolDefinition")] definition: JsValue,
        execute: Function,
        reconcile: Function,
        project: Function,
    ) -> std::result::Result<(), JsValue> {
        let definition: ToolDefinition = from_js(definition)?;
        let host = Arc::new(HostTool {
            execute,
            reconcile,
            project,
        });
        self.tools
            .register(Tool {
                definition,
                executor: host.clone(),
                projection: host,
            })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = registerMachine)]
    pub fn register_machine(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "WasmMachineDefinition")] definition: JsValue,
        initialize: Function,
        transition: Function,
    ) -> std::result::Result<(), JsValue> {
        let definition: WasmMachineDefinition = from_js(definition)?;
        let machine = Arc::new(HostMachine {
            identity: MachineIdentity {
                name: definition.name,
                version: definition.version,
                digest: key_bytes(definition.digest)?,
            },
            state_schema: definition.state_schema,
            initialize,
            transition,
        });
        let mut task = TaskDefinition::<Value, Value>::resumable(
            machine.clone(),
            definition.input_schema,
            definition.output_schema,
        )
        .map_err(js_error)?;
        for requirement in definition.requirements {
            task = task.requires(requirement).map_err(js_error)?;
        }
        self.register_definition(machine, task)
    }

    /// Registers the Rust stock-turn adapter over the ordinary model outbox.
    #[wasm_bindgen(js_name = registerStockTurn)]
    pub fn register_stock_turn(&mut self) -> std::result::Result<(), JsValue> {
        let machine = Arc::new(StockTurnMachine::new());
        let task = machine.wire_definition().map_err(js_error)?;
        self.register_definition(machine, task)
    }
    /// Registers the ordinary bounded fork preparation worker.
    #[wasm_bindgen(js_name = registerForkPreparation)]
    pub fn register_fork_preparation(&mut self) -> std::result::Result<(), JsValue> {
        let machine = Arc::new(crate::filesystem::ForkPreparationMachine::new());
        let definition = machine.wire_definition().map_err(js_error)?;
        self.register_definition(machine, definition)
    }
}

impl WasmTaskRegistry {
    fn register_definition(
        &mut self,
        machine: Arc<dyn ResumableMachine>,
        task: TaskDefinition<Value, Value>,
    ) -> std::result::Result<(), JsValue> {
        // Only NotFound means this exact task key is vacant, including when an occupied key has different Rust types.
        let identity = machine.identity();
        if !matches!(
            self.tasks.get_version::<Value, Value>(
                &identity.name,
                &identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                None
            ),
            Err(Error::NotFound(_))
        ) {
            return Err(js_error(Error::Conflict(
                "task version is already registered".into(),
            )));
        }
        self.machines
            .register(machine, &mut None)
            .map_err(js_error)?;
        self.tasks.register(task).map_err(js_error)?;
        Ok(())
    }
}
