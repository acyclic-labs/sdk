use std::rc::Rc;

use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{Path, Type};

// Both sets keep first-insertion order, so a derive expands to the same tokens in every compiler
// process: a hash-ordered set made the expansion, and with it the crate hash, differ between two
// compiles of one crate (its metadata and its rlib), and dependents then failed with E0460.
pub struct Dependencies {
    crate_rename: Rc<Path>,
    dependencies: Vec<Dependency>,
    types: Vec<Rc<Type>>,
}

#[derive(Eq, PartialEq)]
enum Dependency {
    // A dependency on all dependencies of `ty`.
    // This does not include a dependency on `ty` itself - only its dependencies!
    Transitive {
        crate_rename: Rc<Path>,
        ty: Rc<Type>,
    },
    // A dependency on all type parameters of `ty`, as returned by `TS::generics()`.
    // This does not include a dependency on `ty` itself.
    Generics {
        crate_rename: Rc<Path>,
        ty: Rc<Type>,
    },
    Type(Rc<Type>),
}

impl Dependencies {
    pub fn new(crate_rename: Path) -> Self {
        Self {
            dependencies: Vec::new(),
            crate_rename: Rc::new(crate_rename),
            types: Vec::new(),
        }
    }

    pub fn used_types(&self) -> impl Iterator<Item = &Type> {
        self.types.iter().map(Rc::as_ref)
    }

    /// Adds all dependencies from the given type
    pub fn append_from(&mut self, ty: &Type) {
        let ty = self.push_type(ty);
        self.insert(Dependency::Transitive {
            crate_rename: self.crate_rename.clone(),
            ty,
        });
    }

    /// Adds the given type.
    pub fn push(&mut self, ty: &Type) {
        let ty = self.push_type(ty);
        self.insert(Dependency::Type(ty.clone()));
        self.insert(Dependency::Generics {
            crate_rename: self.crate_rename.clone(),
            ty,
        });
    }

    pub fn append(&mut self, other: Dependencies) {
        for dependency in other.dependencies {
            self.insert(dependency);
        }
        for ty in other.types {
            if !self.types.contains(&ty) {
                self.types.push(ty);
            }
        }
    }

    fn insert(&mut self, dependency: Dependency) {
        if !self.dependencies.contains(&dependency) {
            self.dependencies.push(dependency);
        }
    }

    fn push_type(&mut self, ty: &Type) -> Rc<Type> {
        if let Some(known) = self.types.iter().find(|known| known.as_ref() == ty) {
            return known.clone();
        }
        let ty = Rc::new(ty.clone());
        self.types.push(ty.clone());
        ty
    }
}

impl ToTokens for Dependencies {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let lines = self.dependencies.iter();

        tokens.extend(quote![
            #(#lines;)*
        ]);
    }
}

impl ToTokens for Dependency {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(match self {
            Dependency::Transitive { crate_rename, ty } => {
                quote![<#ty as #crate_rename::TS>::visit_dependencies(v)]
            }
            Dependency::Generics { crate_rename, ty } => {
                quote![<#ty as #crate_rename::TS>::visit_generics(v)]
            }
            Dependency::Type(ty) => quote![v.visit::<#ty>()],
        });
    }
}
