/// Exact platform-native absolute path used only at the kernel callback edge.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MountPath {
    components: Vec<Vec<u8>>,
}

impl MountPath {
    /// Takes ownership of exact root-to-leaf native components.
    #[must_use]
    pub const fn from_components(components: Vec<Vec<u8>>) -> Self {
        Self { components }
    }

    /// Appends one exact native component using the path's existing storage.
    pub fn push_component(&mut self, component: Vec<u8>) {
        self.components.push(component);
    }
    /// Returns the volume root.
    #[must_use]
    pub const fn root() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    /// Appends one exact platform-native component.
    #[must_use]
    pub fn child(&self, component: Vec<u8>) -> Self {
        let mut components = self.components.clone();
        components.push(component);
        Self { components }
    }

    /// Borrows exact root-to-leaf components.
    #[must_use]
    pub fn components(&self) -> &[Vec<u8>] {
        &self.components
    }

    /// Returns the containing directory, or `None` at the volume root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        let (_, parent) = self.components.split_last()?;
        Some(Self {
            components: parent.to_vec(),
        })
    }
}
