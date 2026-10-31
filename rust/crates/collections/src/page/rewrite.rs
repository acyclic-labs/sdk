//! Iterative immutable-page rewrite transitions over the caller's owned frames.
//!
//! The caller performs original page IO and validates its height, identity and
//! bounds. These transitions neither allocate nor wait: stack growth, original
//! admission and result custody stay on the caller's existing work owner.

/// The original page reader either completed a leaf or retained an internal frame.
pub enum Entered<F, O> {
    /// A leaf's original rewrite result.
    Complete(O),
    /// An internal frame whose children still need to be visited.
    Internal(F),
}

/// The next action on the original page IO path.
pub enum Action<R, F, O> {
    /// Read and validate the next selected original child request.
    Enter(R),
    /// Finish the rewrite of this original frame.
    Finish(F),
    /// Hand back the original completed rewrite result.
    Complete(O),
}

/// Borrowed original work and ownership operations for one rewrite.
///
/// Implementations retain their existing frame and stack allocation tokens.
/// No callback may synthesize a new allowance or release an admitted frame's
/// custody while its physical values remain alive.
pub trait Work<I: ?Sized> {
    /// An original selected child request, including inherited bounds.
    type Request;
    /// One original internal rewrite frame.
    type Frame;
    /// Original owned rewritten child summaries.
    type Output;
    /// Original caller-funded frame stack.
    type Stack;
    /// Original admission, cancellation or tree-contract refusal.
    type Error;

    /// Selects the next child, consuming one child position before returning it.
    /// Returns `None` only after all selected children have been consumed.
    fn advance(
        &mut self,
        frame: &mut Self::Frame,
        input: &I,
    ) -> Result<Option<Self::Request>, Self::Error>;
    /// Admits stack growth before retaining this exact original frame.
    fn push(&mut self, stack: &mut Self::Stack, frame: Self::Frame) -> Result<(), Self::Error>;
    /// Removes the original parent frame without allocation or admission.
    fn pop(&mut self, stack: &mut Self::Stack) -> Option<Self::Frame>;
    /// Transfers completed summaries and their original custody into the parent.
    fn append(&mut self, frame: &mut Self::Frame, output: Self::Output) -> Result<(), Self::Error>;
}

/// Continues from the original page read, without recursion or another IO owner.
pub fn descend<I: ?Sized, W: Work<I>>(
    work: &mut W,
    stack: &mut W::Stack,
    entered: Entered<W::Frame, W::Output>,
    input: &I,
) -> Result<Action<W::Request, W::Frame, W::Output>, W::Error> {
    match entered {
        Entered::Complete(output) => ascend(work, stack, output, input),
        Entered::Internal(frame) => follow(work, stack, frame, input),
    }
}

/// Transfers one completed child to its original parent or completes the walk.
/// Every call removes one original stack frame or returns the final result.
pub fn ascend<I: ?Sized, W: Work<I>>(
    work: &mut W,
    stack: &mut W::Stack,
    output: W::Output,
    input: &I,
) -> Result<Action<W::Request, W::Frame, W::Output>, W::Error> {
    let Some(mut frame) = work.pop(stack) else {
        return Ok(Action::Complete(output));
    };
    work.append(&mut frame, output)?;
    follow(work, stack, frame, input)
}

fn follow<I: ?Sized, W: Work<I>>(
    work: &mut W,
    stack: &mut W::Stack,
    mut frame: W::Frame,
    input: &I,
) -> Result<Action<W::Request, W::Frame, W::Output>, W::Error> {
    match work.advance(&mut frame, input)? {
        Some(request) => {
            work.push(stack, frame)?;
            Ok(Action::Enter(request))
        }
        None => Ok(Action::Finish(frame)),
    }
}
