//! Lock construction over parsed source. Known constructors use resolved
//! owners and expected types; unresolved constructions remain indeterminate.
use crate::{Names, items::Items, macros::indeterminate};
use quote::ToTokens;
use std::collections::{HashMap, HashSet};
use syn::{
    Block, Expr, ExprPath, Fields, FnArg, GenericParam, Generics, Local, Pat, Path, ReturnType,
    Signature, Stmt, Type,
    visit::{self, Visit},
};

/// Containers whose `Default` holds no value, so a lock argument is not
/// constructed by it.
const EMPTY: [&str; 9] = [
    "Option",
    "Vec",
    "VecDeque",
    "HashMap",
    "BTreeMap",
    "HashSet",
    "BTreeSet",
    "PhantomData",
    "Weak",
];
const TRAITS: [&str; 5] = ["Default", "From", "Into", "TryFrom", "TryInto"];
const INFERRED: [&str; 5] = ["default", "from", "into", "try_from", "try_into"];
const GENERIC: [&str; 4] = ["new", "default", "from", "try_from"];
const PLAIN: [&str; 22] = [
    "bool", "char", "str", "String", "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16",
    "i32", "i64", "i128", "isize", "f32", "f64", "Arc", "Rc", "Box", "Cell",
];
/// Methods whose result type is inferred from where it goes, or which make
/// a `Default` value of a type parameter.
const METHODS: [&str; 5] = [
    "into",
    "try_into",
    "unwrap_or_default",
    "or_default",
    "get_or_insert_default",
];

/// Whether `file` and the macro statements `stmts` construct a lock.
pub fn find<'ast>(
    file: &'ast syn::File,
    stmts: &[&'ast Stmt],
    names: &Names<'ast>,
) -> syn::Result<bool> {
    let mut items = Items::default();
    items.visit_file(file);
    stmts.iter().for_each(|stmt| items.visit_stmt(stmt));
    let mut finder = Finder {
        names,
        items,
        found: false,
        unresolved: Vec::new(),
        expected: HashMap::new(),
        callees: HashSet::new(),
        generics: Vec::new(),
        returns: Vec::new(),
        selves: Vec::new(),
    };
    finder.visit_file(file);
    stmts.iter().for_each(|stmt| finder.visit_stmt(stmt));
    match finder.unresolved.first() {
        Some(expr) => Err(indeterminate(&format!(
            "the type `{expr}` constructs is inferred from context the scan cannot resolve; name the type"
        ))),
        None => Ok(finder.found),
    }
}

/// The last segment of an unqualified type path.
pub fn type_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) if path.qself.is_none() => last(&path.path),
        Type::Paren(inner) => type_name(&inner.elem),
        Type::Group(inner) => type_name(&inner.elem),
        _ => None,
    }
}

fn last(path: &Path) -> Option<String> {
    path.segments
        .last()
        .map(|segment| crate::name(&segment.ident))
}

fn key(expr: &Expr) -> *const Expr {
    expr
}

#[derive(Clone, Copy)]
enum Expect<'ast> {
    Type(&'ast Type),
    Known(bool),
}

struct Finder<'a, 'ast> {
    names: &'a Names<'ast>,
    items: Items<'ast>,
    found: bool,
    unresolved: Vec<String>,
    expected: HashMap<*const Expr, Expect<'ast>>,
    callees: HashSet<*const Expr>,
    generics: Vec<String>,
    returns: Vec<Option<Expect<'ast>>>,
    selves: Vec<&'ast Type>,
}

impl<'ast> Finder<'_, 'ast> {
    fn generic(&self, name: &str) -> bool {
        self.generics.iter().any(|generic| generic == name)
    }

    /// Whether `ty` is itself a lock; `None` when it is a generic parameter,
    /// `_` or `impl Trait`.
    fn is_lock(&self, ty: &Type) -> Option<bool> {
        self.is_lock_at(ty, 0)
    }

    fn is_lock_at(&self, ty: &Type, depth: usize) -> Option<bool> {
        if depth > 16 {
            return None;
        }
        match ty {
            Type::Paren(inner) => self.is_lock_at(&inner.elem, depth + 1),
            Type::Group(inner) => self.is_lock_at(&inner.elem, depth + 1),
            Type::Infer(_) | Type::ImplTrait(_) | Type::Macro(_) | Type::Verbatim(_) => None,
            Type::Path(path) if path.qself.is_some() => None,
            Type::Path(path) => {
                let name = last(&path.path)?;
                if path.path.segments.len() == 1 && self.generic(&name) {
                    None
                } else if path.path.is_ident("Self") {
                    self.selves
                        .last()
                        .and_then(|ty| self.is_lock_at(ty, depth + 1))
                } else {
                    let path = path
                        .path
                        .segments
                        .iter()
                        .map(|segment| crate::name(&segment.ident))
                        .collect::<Vec<_>>()
                        .join("::");
                    let name = self.names.resolve(&path)?;
                    let terminal = name.rsplit("::").next()?;
                    let builtin = !name.contains("::")
                        || name.starts_with("std::")
                        || name.starts_with("core::")
                        || name.starts_with("alloc::");
                    if let Some(expanded) = self.names.instantiated(ty) {
                        self.is_lock_at(&expanded, depth + 1)
                    } else if crate::LOCKS.contains(&terminal) {
                        Some(true)
                    } else if builtin && (PLAIN.contains(&terminal) || EMPTY.contains(&terminal))
                        || self.items.structs.contains_key(&name)
                        || self.names.alias(&name).is_some()
                    {
                        Some(false)
                    } else {
                        None
                    }
                }
            }
            Type::Array(_)
            | Type::BareFn(_)
            | Type::Never(_)
            | Type::Ptr(_)
            | Type::Reference(_)
            | Type::Slice(_)
            | Type::Tuple(_) => Some(false),
            _ => None,
        }
    }

    /// Whether a value of type `ty` made by `Default` holds a lock by value:
    /// a lock, a tuple or array of one, an alias of one, or a type with a lock
    /// argument other than an [`EMPTY`] container.
    fn holds(&self, ty: &Type, depth: usize) -> Option<bool> {
        if depth > 16 {
            return None;
        }
        if let Some(expanded) = self.names.instantiated(ty) {
            return self.holds(&expanded, depth + 1);
        }
        match ty {
            Type::Paren(inner) => self.holds(&inner.elem, depth + 1),
            Type::Group(inner) => self.holds(&inner.elem, depth + 1),
            Type::Tuple(tuple) => any(tuple.elems.iter().map(|ty| self.holds(ty, depth + 1))),
            Type::Array(array) => match &array.len {
                Expr::Lit(literal) => match &literal.lit {
                    syn::Lit::Int(length) => match length.base10_parse::<usize>().ok()? {
                        0 => Some(false),
                        _ => self.holds(&array.elem, depth + 1),
                    },
                    _ => None,
                },
                _ => None,
            },
            Type::Path(path) if path.qself.is_none() => {
                if self.is_lock_at(ty, depth)? {
                    return Some(true);
                }
                let resolved = self.names.path(&path.path)?;
                let spelling = resolved
                    .segments
                    .iter()
                    .map(|segment| crate::name(&segment.ident))
                    .collect::<Vec<_>>()
                    .join("::");
                let segment = resolved.segments.last()?;
                let name = crate::name(&segment.ident);
                if EMPTY.contains(&name.as_str()) {
                    return Some(false);
                }
                if let Some(Some(item)) = self.items.structs.get(&spelling) {
                    let types: Vec<_> = item.fields.iter().map(|field| &field.ty).collect();
                    let target: Type = syn::parse_quote!((#(#types,)*));
                    let expanded = crate::substitute(&item.generics, &segment.arguments, &target);
                    return self.holds(&expanded, depth + 1);
                }
                match &segment.arguments {
                    syn::PathArguments::AngleBracketed(angle) => {
                        any(angle.args.iter().filter_map(|arg| match arg {
                            syn::GenericArgument::Type(ty) => Some(self.holds(ty, depth + 1)),
                            _ => None,
                        }))
                    }
                    _ => Some(false),
                }
            }
            _ => self.is_lock_at(ty, depth),
        }
    }

    fn assign(&mut self, expr: &'ast Expr, expect: Option<Expect<'ast>>) {
        let element = |index: usize| match expect {
            Some(Expect::Type(Type::Tuple(tuple))) => {
                tuple.elems.iter().nth(index).map(Expect::Type)
            }
            Some(Expect::Type(Type::Array(array))) => Some(Expect::Type(&array.elem)),
            _ => None,
        };
        match expr {
            Expr::Paren(inner) => self.assign(&inner.expr, expect),
            Expr::Group(inner) => self.assign(&inner.expr, expect),
            Expr::Block(block) => self.tail(&block.block, expect),
            Expr::Unsafe(block) => self.tail(&block.block, expect),
            Expr::Const(block) => self.tail(&block.block, expect),
            Expr::If(branch) => {
                self.tail(&branch.then_branch, expect);
                if let Some((_, otherwise)) = &branch.else_branch {
                    self.assign(otherwise, expect);
                }
            }
            Expr::Match(choice) => {
                for arm in &choice.arms {
                    self.assign(&arm.body, expect);
                }
            }
            Expr::Tuple(tuple) => {
                for (index, elem) in tuple.elems.iter().enumerate() {
                    self.assign(elem, element(index));
                }
            }
            Expr::Array(array) => {
                for elem in &array.elems {
                    self.assign(elem, element(0));
                }
            }
            Expr::Repeat(repeat) => self.assign(&repeat.expr, element(0)),
            Expr::Reference(reference) => {
                let inner = match expect {
                    Some(Expect::Type(Type::Reference(ty))) => Some(Expect::Type(&ty.elem)),
                    _ => None,
                };
                self.assign(&reference.expr, inner);
            }
            _ => {
                if let Some(expect) = expect {
                    self.expected.insert(key(expr), expect);
                }
            }
        }
    }

    fn tail(&mut self, block: &'ast Block, expect: Option<Expect<'ast>>) {
        if let Some(Stmt::Expr(expr, None)) = block.stmts.last() {
            self.assign(expr, expect);
        }
    }

    fn output(output: &'ast ReturnType) -> Expect<'ast> {
        match output {
            ReturnType::Default => Expect::Known(false),
            ReturnType::Type(_, ty) => Expect::Type(ty),
        }
    }

    /// Visits a function body with its generics and return type in scope.
    fn function(
        &mut self,
        signature: &'ast Signature,
        block: &'ast Block,
        visit: impl FnOnce(&mut Self),
    ) {
        let scope = self.generics.len();
        self.push(&signature.generics);
        let output = Self::output(&signature.output);
        self.tail(block, Some(output));
        self.returns.push(Some(output));
        visit(self);
        self.returns.pop();
        self.generics.truncate(scope);
    }

    fn push(&mut self, generics: &Generics) {
        for param in &generics.params {
            if let GenericParam::Type(param) = param {
                self.generics.push(crate::name(&param.ident));
            }
        }
    }

    fn judge(&mut self, expr: &'ast Expr) {
        let verdict = match self.expected.get(&key(expr)) {
            Some(Expect::Known(lock)) => Some(*lock),
            Some(Expect::Type(ty)) => self.holds(ty, 0),
            None => None,
        };
        match verdict {
            Some(lock) => self.found |= lock,
            None => self.unresolved.push(expr.to_token_stream().to_string()),
        }
    }

    // A context-typed constructor path: `Default::default`, `From::from`,
    // `<_>::default`, `T::new` for a generic `T`, ...
    fn inferred(&self, path: &ExprPath) -> bool {
        let Some(resolved) = self.names.path(&path.path) else {
            return true;
        };
        let segments = &resolved.segments;
        let Some(name) = segments.last().map(|segment| crate::name(&segment.ident)) else {
            return true;
        };
        if let Some(qself) = &path.qself {
            return INFERRED.contains(&name.as_str()) && self.is_lock(&qself.ty).is_none();
        }
        let spelling = segments
            .iter()
            .map(|segment| crate::name(&segment.ident))
            .collect::<Vec<_>>()
            .join("::");
        if matches!(spelling.as_str(), "std::mem::take" | "core::mem::take")
            || (segments.len() == 1
                && (INFERRED.contains(&name.as_str()) || GENERIC.contains(&name.as_str())))
        {
            return true;
        }
        let Some(owner) = segments.len().checked_sub(2).map(|at| &segments[at]) else {
            return false;
        };
        let owner_name = crate::name(&owner.ident);
        (INFERRED.contains(&name.as_str())
            && TRAITS.contains(&owner_name.rsplit("::").next().unwrap_or(&owner_name)))
            || (name == "take" && owner_name == "mem")
            || (segments.len() == 2
                && self.generic(&owner_name)
                && GENERIC.contains(&name.as_str()))
    }

    // A constructor named on a lock: `Lock::new`, `<Lock<T>>::new`,
    // `<Lock<T> as Default>::default`, `Self::new` in an impl for a lock, and
    // `default` of a type holding a lock, such as `Arc::<Lock<T>>::default`.
    fn constructor(&self, path: &ExprPath) -> Option<bool> {
        let resolved = self.names.path(&path.path)?;
        let segments = &resolved.segments;
        let Some(name) = segments.last().map(|segment| crate::name(&segment.ident)) else {
            return Some(false);
        };
        if !GENERIC.contains(&name.as_str()) {
            return Some(false);
        }
        if let Some(qself) = &path.qself {
            // A user trait may return any associated type, independently of
            // its implementor. Only known conversion traits determine ownership.
            if qself.position > 0 {
                let owner = segments
                    .iter()
                    .take(qself.position)
                    .map(|segment| crate::name(&segment.ident))
                    .collect::<Vec<_>>()
                    .join("::");
                if !TRAITS.contains(&owner.rsplit("::").next()?) {
                    return None;
                }
            }
            return if name == "default" {
                self.holds(&qself.ty, 0)
            } else {
                self.is_lock(&qself.ty)
            };
        }
        if segments.len() < 2 {
            return None;
        }
        let owner_type = Type::Path(syn::TypePath {
            qself: None,
            path: Path {
                leading_colon: resolved.leading_colon,
                segments: segments.iter().take(segments.len() - 1).cloned().collect(),
            },
        });
        if name == "default" {
            self.holds(&owner_type, 0)
        } else {
            self.is_lock(&owner_type)
        }
    }

    fn signature(&self, func: &Expr) -> Option<&'ast Signature> {
        let Expr::Path(path) = func else { return None };
        let resolved = self.names.path(&path.path)?;
        let segments: Vec<String> = resolved
            .segments
            .iter()
            .map(|segment| crate::name(&segment.ident))
            .collect();
        let name = match segments.as_slice() {
            [name] => name.clone(),
            [owner, name] if owner == "Self" => {
                format!("{}::{name}", type_name(self.selves.last()?)?)
            }
            [owner, name] => format!("{owner}::{name}"),
            _ => return None,
        };
        self.items.functions.get(&name).copied().flatten()
    }

    fn fields(&self, path: &Path) -> Option<&'ast Fields> {
        let name = (path.segments.len() == 1).then(|| last(path))??;
        self.items
            .structs
            .get(&name)
            .copied()
            .flatten()
            .map(|item| &item.fields)
    }

    fn field_types(&self, fields: &'ast Fields) -> Vec<(Option<String>, &'ast Type)> {
        fields
            .iter()
            .map(|field| (field.ident.as_ref().map(ToString::to_string), &field.ty))
            .collect()
    }
}

/// `const _: fn(A) -> B = |lock| lock;`, which asserts at compile time that
/// `A` is `B` and constructs nothing; its types are not written values.
fn identity(item: &syn::ItemConst) -> bool {
    let Expr::Closure(closure) = &*item.expr else {
        return false;
    };
    let parameter = match closure.inputs.iter().collect::<Vec<_>>().as_slice() {
        [Pat::Ident(parameter)] => crate::name(&parameter.ident),
        _ => return false,
    };
    item.ident == "_"
        && matches!(*item.ty, Type::BareFn(_))
        && matches!(&*closure.body, Expr::Path(body) if body.path.is_ident(&parameter))
}

fn any(verdicts: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut known = Some(false);
    for verdict in verdicts {
        match verdict {
            Some(true) => return Some(true),
            None => known = None,
            Some(false) => {}
        }
    }
    known
}

impl<'ast> Visit<'ast> for Finder<'_, 'ast> {
    fn visit_expr(&mut self, expr: &'ast Expr) {
        match expr {
            Expr::Call(call) => {
                if let Expr::Path(path) = &*call.func {
                    self.callees.insert(key(&call.func));
                    if self.signature(&call.func).is_none() && self.fields(&path.path).is_none() {
                        if self.inferred(path) {
                            self.judge(expr);
                        } else {
                            let resolved = self.names.path(&path.path);
                            let spelling = resolved.as_ref().map(|path| {
                                path.segments
                                    .iter()
                                    .map(|segment| crate::name(&segment.ident))
                                    .collect::<Vec<_>>()
                                    .join("::")
                            });
                            let method = resolved.as_ref().and_then(last);
                            // These language/library entries do not manufacture a
                            // lock; their executable arguments are visited below.
                            let transparent =
                                matches!(spelling.as_deref(), Some("Some" | "Ok" | "Err"));
                            let transparent = transparent
                                || resolved.as_ref().is_some_and(|path| {
                                    path.segments.len() == 2
                                        && path.segments[0].ident == "loom"
                                        && path.segments[1].ident == "model"
                                });
                            if !transparent
                                && !method
                                    .as_deref()
                                    .is_some_and(|method| GENERIC.contains(&method))
                            {
                                self.unresolved.push(expr.to_token_stream().to_string());
                            }
                        }
                    }
                }
                if !matches!(&*call.func, Expr::Path(_) | Expr::Closure(_)) {
                    self.unresolved.push(expr.to_token_stream().to_string());
                }
                let inputs: Vec<Option<Expect<'ast>>> =
                    if let Some(signature) = self.signature(&call.func) {
                        signature
                            .inputs
                            .iter()
                            .map(|input| match input {
                                FnArg::Typed(typed) => Some(Expect::Type(&typed.ty)),
                                FnArg::Receiver(_) => None,
                            })
                            .collect()
                    } else if let Expr::Path(path) = &*call.func
                        && let Some(fields) = self.fields(&path.path)
                    {
                        self.field_types(fields)
                            .into_iter()
                            .map(|(_, ty)| Some(Expect::Type(ty)))
                            .collect()
                    } else {
                        Vec::new()
                    };
                if inputs.len() == call.args.len() {
                    for (arg, expect) in call.args.iter().zip(inputs) {
                        self.assign(arg, expect);
                    }
                }
            }
            Expr::MethodCall(call)
                if METHODS.contains(&crate::name(&call.method).as_str())
                    && call.turbofish.is_none()
                    && call.args.is_empty() =>
            {
                self.judge(expr);
            }
            Expr::MethodCall(_) => self.unresolved.push(expr.to_token_stream().to_string()),
            Expr::Verbatim(_) => self.unresolved.push(expr.to_token_stream().to_string()),
            Expr::Path(path) => {
                if !self.callees.contains(&key(expr))
                    && self
                        .names
                        .path(&path.path)
                        .is_some_and(|path| path.segments.len() > 1)
                    && !self.inferred(path)
                {
                    self.unresolved.push(expr.to_token_stream().to_string());
                }
                match self.constructor(path) {
                    Some(found) => self.found |= found,
                    None if self.names.path(&path.path).is_none() || !self.inferred(path) => {
                        self.unresolved.push(expr.to_token_stream().to_string());
                    }
                    None if !self.callees.contains(&key(expr)) => self.judge(expr),
                    None => {}
                }
                if path
                    .qself
                    .as_ref()
                    .is_some_and(|qself| path.path.segments.len() > qself.position + 1)
                {
                    self.unresolved.push(expr.to_token_stream().to_string());
                } else if !self.callees.contains(&key(expr)) && self.inferred(path) {
                    self.judge(expr);
                }
            }
            Expr::Struct(literal) => {
                let ty = Type::Path(syn::TypePath {
                    qself: literal.qself.clone(),
                    path: literal.path.clone(),
                });
                match self.is_lock(&ty) {
                    Some(found) => self.found |= found,
                    None => self.unresolved.push(expr.to_token_stream().to_string()),
                }
                if let Some(fields) = self.fields(&literal.path) {
                    let types = self.field_types(fields);
                    for value in &literal.fields {
                        let member = match &value.member {
                            syn::Member::Named(name) => types
                                .iter()
                                .find(|(field, _)| field.as_deref() == Some(&name.to_string()))
                                .map(|(_, ty)| *ty),
                            syn::Member::Unnamed(index) => {
                                types.get(index.index as usize).map(|(_, ty)| *ty)
                            }
                        };
                        self.assign(&value.expr, member.map(Expect::Type));
                    }
                }
                if let Some(rest) = &literal.rest {
                    let ty = Type::Path(syn::TypePath {
                        qself: None,
                        path: literal.path.clone(),
                    });
                    let holds = self.holds(&ty, 0);
                    self.assign(rest, holds.map(Expect::Known));
                }
            }
            Expr::Return(returned) => {
                if let Some(value) = &returned.expr {
                    let expect = self.returns.last().copied().flatten();
                    self.assign(value, expect);
                }
            }
            Expr::Closure(closure) => {
                let output = match &closure.output {
                    ReturnType::Type(_, ty) => Some(Expect::Type(ty)),
                    ReturnType::Default => None,
                };
                self.assign(&closure.body, output);
                self.returns.push(output);
                visit::visit_expr(self, expr);
                self.returns.pop();
                return;
            }
            Expr::Async(_) => {
                self.returns.push(None);
                visit::visit_expr(self, expr);
                self.returns.pop();
                return;
            }
            Expr::Array(_)
            | Expr::Assign(_)
            | Expr::Await(_)
            | Expr::Binary(_)
            | Expr::Block(_)
            | Expr::Break(_)
            | Expr::Cast(_)
            | Expr::Const(_)
            | Expr::Continue(_)
            | Expr::Field(_)
            | Expr::ForLoop(_)
            | Expr::Group(_)
            | Expr::If(_)
            | Expr::Index(_)
            | Expr::Infer(_)
            | Expr::Let(_)
            | Expr::Lit(_)
            | Expr::Loop(_)
            | Expr::Macro(_)
            | Expr::Match(_)
            | Expr::Paren(_)
            | Expr::Range(_)
            | Expr::RawAddr(_)
            | Expr::Reference(_)
            | Expr::Repeat(_)
            | Expr::Try(_)
            | Expr::TryBlock(_)
            | Expr::Tuple(_)
            | Expr::Unary(_)
            | Expr::Unsafe(_)
            | Expr::While(_)
            | Expr::Yield(_) => {}
            _ => self.unresolved.push(expr.to_token_stream().to_string()),
        }
        visit::visit_expr(self, expr);
    }

    // `let lock: Lock<T> = ..;` declares a lock whatever initializes it.
    fn visit_local(&mut self, local: &'ast Local) {
        let ty = match &local.pat {
            Pat::Type(typed) => Some(&*typed.ty),
            _ => None,
        };
        if let Some(ty) = ty {
            self.found |= self.is_lock(ty) == Some(true);
            // Macro-generated typed inputs have no source initializer. Their
            // Arbitrary implementation may populate even an empty-by-default
            // container, so Default's empty-container rule cannot judge them.
            if local.init.is_none() && self.is_lock(ty) != Some(true) {
                match ty {
                    Type::Path(path)
                        if path.qself.is_none()
                            && path.path.segments.last().is_some_and(|segment| {
                                matches!(segment.arguments, syn::PathArguments::None)
                            })
                            && self.is_lock(ty).is_some() => {}
                    _ => self.unresolved.push(ty.to_token_stream().to_string()),
                }
            }
        }
        if let Some(init) = &local.init {
            self.assign(&init.expr, ty.map(Expect::Type));
        }
        visit::visit_local(self, local);
    }

    fn visit_item_static(&mut self, item: &'ast syn::ItemStatic) {
        self.found |= self.is_lock(&item.ty) == Some(true);
        self.assign(&item.expr, Some(Expect::Type(&item.ty)));
        visit::visit_item_static(self, item);
    }

    fn visit_item_const(&mut self, item: &'ast syn::ItemConst) {
        if identity(item) {
            return;
        }
        self.found |= self.is_lock(&item.ty) == Some(true);
        self.assign(&item.expr, Some(Expect::Type(&item.ty)));
        visit::visit_item_const(self, item);
    }

    fn visit_impl_item_const(&mut self, item: &'ast syn::ImplItemConst) {
        self.found |= self.is_lock(&item.ty) == Some(true);
        self.assign(&item.expr, Some(Expect::Type(&item.ty)));
        visit::visit_impl_item_const(self, item);
    }

    // A field holding a lock: the type owns one, and a derived `Default`
    // constructs it.
    fn visit_field(&mut self, field: &'ast syn::Field) {
        self.found |= self.is_lock(&field.ty) == Some(true);
        visit::visit_field(self, field);
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        self.function(&item.sig, &item.block, |finder| {
            visit::visit_item_fn(finder, item);
        });
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        self.function(&item.sig, &item.block, |finder| {
            visit::visit_impl_item_fn(finder, item);
        });
    }

    fn visit_trait_item_fn(&mut self, item: &'ast syn::TraitItemFn) {
        let scope = self.generics.len();
        self.push(&item.sig.generics);
        if let Some(block) = &item.default {
            self.tail(block, Some(Self::output(&item.sig.output)));
        }
        self.returns.push(Some(Self::output(&item.sig.output)));
        visit::visit_trait_item_fn(self, item);
        self.returns.pop();
        self.generics.truncate(scope);
    }

    fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
        let scope = self.generics.len();
        self.push(&item.generics);
        self.selves.push(&item.self_ty);
        visit::visit_item_impl(self, item);
        self.selves.pop();
        self.generics.truncate(scope);
    }

    fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
        let scope = self.generics.len();
        self.push(&item.generics);
        // `Self` in a trait is any implementor.
        self.generics.push("Self".into());
        visit::visit_item_trait(self, item);
        self.generics.truncate(scope);
    }

    fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
        let scope = self.generics.len();
        self.push(&item.generics);
        visit::visit_item_struct(self, item);
        self.generics.truncate(scope);
    }

    fn visit_item_enum(&mut self, item: &'ast syn::ItemEnum) {
        let scope = self.generics.len();
        self.push(&item.generics);
        visit::visit_item_enum(self, item);
        self.generics.truncate(scope);
    }
}
