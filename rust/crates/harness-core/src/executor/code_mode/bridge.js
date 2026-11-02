(rpc, definitions) => {
    const parse = JSON.parse, stringify = JSON.stringify;
    const ErrorType = Error, PromiseType = Promise;
    const define = Object.defineProperty, tools = Object.create(null);
    const then = Promise.prototype.then;
    const constructor = Object.freeze({[Symbol.species]: PromiseType});
    // Await only private promises with a stable constructor. User
    // mutations of Promise.prototype cannot replace their await.
    const stable = promise => define(promise, 'constructor', {value: PromiseType});
    let pending = 0, wake;
    let closed = false;
    const invoke = async (name, args) => {
        pending++;
        try {
            const response = await stable(rpc(name, stringify(args)));
            if (name === null) return;
            const reply = parse(response);
            if (!reply.ok) throw new ErrorType(reply.value);
            return reply.value;
        } finally {
            if (--pending === 0 && wake) wake();
        }
    };
    for (const definition of parse(definitions)) {
        const tool = args => {
            if (closed) throw new ErrorType('JavaScript calls are closed');
            return invoke(definition.name, args);
        };
        Object.defineProperty(tool, 'input_schema', {value: definition.input_schema});
        tools[definition.name] = Object.freeze(tool);
    }
    Object.defineProperty(globalThis, 'tools', {value: Object.freeze(tools)});
    Object.defineProperty(globalThis, 'console', {value: Object.freeze({
        log: (...values) => {
            if (closed) throw new ErrorType('JavaScript calls are closed');
            return invoke(null, values);
        }
    })});
    return {
        complete: async value => ({value: await value}),
        // The Rust promise future attaches its waker through then.
        protect: promise => define(define(promise, 'constructor', {value: constructor}), 'then', {value: then}),
        drain: async () => {
            while (pending) await stable(new PromiseType(resolve => { wake = resolve; }));
            closed = true;
        }
    };
}
