function (seed, names) {
    "use strict";
    const enqueue = globalThis.__enqueue;
    delete globalThis.__enqueue;
    // Blocking shared-memory waits have no owned host producer. GC liveness
    // is nondeterministic and cannot participate in replayed control flow.
    for (const name of ["Atomics", "SharedArrayBuffer", "WeakRef", "FinalizationRegistry"]) {
        delete globalThis[name];
    }
    const pending = new Map();
    const host = Object.create(null);
    for (const name of names) {
        const call = function (arguments_) {
            return new Promise((resolve, reject) => {
                const ordinal = enqueue(name, JSON.stringify(arguments_));
                pending.set(ordinal, {resolve, reject});
            });
        };
        Object.defineProperty(host, name, {value: call, enumerable: true});
        if (!(name in globalThis)) {
            Object.defineProperty(globalThis, name, {value: call});
        }
    }
    Object.defineProperty(globalThis, "tools", {value: Object.freeze(host)});
    // No host clock or entropy enters the realm, including Date() without new.
    const OriginalDate = Date;
    const clock = seed;
    const parseDate = function (text) {
        if (!/^\d{4}-\d\d-\d\d(?:T.*(?:Z|[+-]\d\d:\d\d))?$/.test(text)) {
            throw new TypeError("Date strings require an explicit UTC offset");
        }
        return OriginalDate.parse(text);
    };
    function SeededDate(...args) {
        if (!new.target) return new OriginalDate(clock).toISOString();
        let instant = clock;
        if (args.length > 1) instant = OriginalDate.UTC(...args);
        else if (args.length) instant = typeof args[0] === "string" ? parseDate(args[0]) : args[0];
        return Reflect.construct(OriginalDate, [instant], new.target);
    }
    SeededDate.UTC = OriginalDate.UTC;
    SeededDate.parse = parseDate;
    SeededDate.prototype = OriginalDate.prototype;
    SeededDate.now = () => clock;
    // Remove the inherited original constructor escape back to the real clock.
    Object.defineProperty(OriginalDate.prototype, "constructor", {value: SeededDate});
    Object.defineProperty(globalThis, "Date", {value: SeededDate});
    for (const suffix of ["Date", "Day", "FullYear", "Hours", "Milliseconds", "Minutes", "Month", "Seconds"]) {
        OriginalDate.prototype["get" + suffix] = OriginalDate.prototype["getUTC" + suffix];
        if (suffix !== "Day") {
            OriginalDate.prototype["set" + suffix] = OriginalDate.prototype["setUTC" + suffix];
        }
    }
    OriginalDate.prototype.getTimezoneOffset = () => 0;
    OriginalDate.prototype.getYear = function () {return this.getUTCFullYear() - 1900;};
    OriginalDate.prototype.setYear = function (year) {
        return this.setUTCFullYear(year >= 0 && year <= 99 ? year + 1900 : year);
    };
    let random = seed;
    Object.defineProperty(Math, "random", {value: function () {
        random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
        return random / 4294967296;
    }});
    // QuickJS has no I/O modules or host functions beyond the closures above.
    // Locale defaults can depend on the process: use deterministic UTC spelling.
    for (const name of ["toString", "toDateString", "toTimeString", "toLocaleString",
                        "toLocaleDateString", "toLocaleTimeString"]) {
        Object.defineProperty(OriginalDate.prototype, name, {
            value: function () { return this.toISOString(); }
        });
    }
    let state = "pending";
    Object.defineProperty(globalThis, "__run", {value: function (run) {
        Promise.resolve().then(run).then(value => {
            state = JSON.stringify(value === undefined ? null : value);
        }, () => {state = "rejected";});
    }});
    Object.defineProperty(globalThis, "__state", {value: () => state});
    Object.defineProperty(globalThis, "__complete", {value: function (ordinal, json) {
        const call = pending.get(ordinal);
        if (!call) throw new Error("unknown host-call ordinal");
        pending.delete(ordinal);
        call.resolve(JSON.parse(json));
    }});
}
