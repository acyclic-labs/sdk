// Suppression of a false positive clippy lint. Upstream issue:
//
//   mutable_key_type false positive for raw pointers
//   https://github.com/rust-lang/rust-clippy/issues/6745
//
// We use `*const MatchPattern` as key in our `SearchCache` hash map.
// Clippy thinks this is a problem since `MatchPattern` has interior mutability
// via `MatchPattern::regex::regex` which is an `AtomicLazyCell`.
// But raw pointers are hashed via the pointer itself, not what is pointed to.
// See https://github.com/rust-lang/rust/blob/1.54.0/library/core/src/hash/mod.rs#L717-L725
#![allow(clippy::mutable_key_type)]

use super::regex::Region;
use super::scope::*;
use super::syntax_definition::*;
use crate::parsing::syntax_definition::ContextId;
use crate::parsing::syntax_set::{SyntaxReference, SyntaxSet};
use fnv::FnvHasher;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

/// Errors that can occur while parsing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParsingError {
    #[error("Somehow main context was popped from the stack")]
    MissingMainContext,
    /// A context is missing. Usually caused by a syntax referencing a another
    /// syntax that is not known to syntect. See e.g. <https://github.com/trishume/syntect/issues/421>
    #[error("Missing context with ID '{0:?}'")]
    MissingContext(ContextId),
    #[error("Bad index to match_at: {0}")]
    BadMatchIndex(usize),
    #[error("Tried to use a ContextReference that has not bee resolved yet: {0:?}")]
    UnresolvedContextReference(ContextReference),
}

/// Keeps the current parser state (the internal syntax interpreter stack) between lines of parsing.
///
/// If you are parsing an entire file you create one of these at the start and use it
/// all the way to the end.
///
/// # Caching
///
/// One reason this is exposed is that since it implements `Clone` you can actually cache
/// these (probably along with a [`HighlightState`]) and only re-start parsing from the point of a change.
/// See the docs for [`HighlightState`] for more in-depth discussion of caching.
///
/// This state doesn't keep track of the current scope stack and parsing only returns changes to this stack
/// so if you want to construct scope stacks you'll need to keep track of that as well.
/// Note that [`HighlightState`] contains exactly this as a public field that you can use.
///
/// **Note:** Caching is for advanced users who have tons of time to maximize performance or want to do so eventually.
/// It is not recommended that you try caching the first time you implement highlighting.
///
/// [`HighlightState`]: ../highlighting/struct.HighlightState.html
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParseState {
    stack: Vec<StateLevel>,
    first_line: bool,
    // See issue #101. Contains indices of frames pushed by `with_prototype`s.
    // Doesn't look at `with_prototype`s below top of stack.
    proto_starts: Vec<usize>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct StateLevel {
    context: ContextId,
    prototypes: Vec<ContextId>,
    captures: Option<(Region, String)>,
}

#[derive(Debug)]
struct RegexMatch<'a> {
    regions: Region,
    context: &'a Context,
    pat_index: usize,
    from_with_prototype: bool,
    would_loop: bool,
}

/// Maps the pattern to the start index, which is -1 if not found.
type SearchCache = HashMap<*const MatchPattern, Option<Region>, BuildHasherDefault<FnvHasher>>;

// To understand the implementation of this, here's an introduction to how
// Sublime Text syntax definitions work.
//
// Let's say we have the following made-up syntax definition:
//
//     contexts:
//       main:
//         - match: A
//           scope: scope.a.first
//           push: context-a
//         - match: b
//           scope: scope.b
//         - match: \w+
//           scope: scope.other
//       context-a:
//         - match: a+
//           scope: scope.a.rest
//         - match: (?=.)
//           pop: true
//
// There are two contexts, `main` and `context-a`. Each context contains a list
// of match rules with instructions for how to proceed.
//
// Let's say we have the input string " Aaaabxxx". We start at position 0 in
// the string. We keep a stack of contexts, which at the beginning is just main.
//
// So we start by looking at the top of the context stack (main), and look at
// the rules in order. The rule that wins is the first one that matches
// "earliest" in the input string. In our example:
//
// 1. The first one matches "A". Note that matches are not anchored, so this
//    matches at position 1.
// 2. The second one matches "b", so position 5. The first rule is winning.
// 3. The third one matches "\w+", so also position 1. But because the first
//    rule comes first, it wins.
//
// So now we execute the winning rule. Whenever we matched some text, we assign
// the scope (if there is one) to the matched text and advance our position to
// after the matched text. The scope is "scope.a.first" and our new position is
// after the "A", so 2. The "push" means that we should change our stack by
// pushing `context-a` on top of it.
//
// In the next step, we repeat the above, but now with the rules in `context-a`.
// The result is that we match "a+" and assign "scope.a.rest" to "aaa", and our
// new position is now after the "aaa". Note that there was no instruction for
// changing the stack, so we stay in that context.
//
// In the next step, the first rule doesn't match anymore, so we go to the next
// rule where "(?=.)" matches. The instruction is to "pop", which means we
// pop the top of our context stack, which means we're now back in main.
//
// This time in main, we match "b", and in the next step we match the rest with
// "\w+", and we're done.
//
//
// ## Preventing loops
//
// These are the basics of how matching works. Now, you saw that you can write
// patterns that result in an empty match and don't change the position. These
// are called non-consuming matches. The problem with them is that they could
// result in infinite loops. Let's look at a syntax where that is the case:
//
//     contexts:
//       main:
//         - match: (?=.)
//           push: test
//       test:
//         - match: \w+
//           scope: word
//         - match: (?=.)
//           pop: true
//
// This is a bit silly, but it's a minimal example for explaining how matching
// works in that case.
//
// Let's say we have the input string " hello". In `main`, our rule matches and
// we go into `test` and stay at position 0. Now, the best match is the rule
// with "pop". But if we used that rule, we'd pop back to `main` and would still
// be at the same position we started at! So this would be an infinite loop,
// which we don't want.
//
// So what Sublime Text does in case a looping rule "won":
//
// * If there's another rule that matches at the same position and does not
//   result in a loop, use that instead.
// * Otherwise, go to the next position and go through all the rules in the
//   current context again. Note that it means that the "pop" could again be the
//   winning rule, but that's ok as it wouldn't result in a loop anymore.
//
// So in our input string, we'd skip one character and try to match the rules
// again. This time, the "\w+" wins because it comes first.

impl ParseState {
    /// Creates a state from a syntax definition, keeping its own reference-counted point to the
    /// main context of the syntax
    pub fn new(syntax: &SyntaxReference) -> ParseState {
        let start_state = StateLevel {
            context: syntax.context_ids()["__start"],
            prototypes: Vec::new(),
            captures: None,
        };
        ParseState {
            stack: vec![start_state],
            first_line: true,
            proto_starts: Vec::new(),
        }
    }

    /// Parses a single line of the file. Because of the way regex engines work you unfortunately
    /// have to pass in a single line contiguous in memory. This can be bad for really long lines.
    /// Sublime Text avoids this by just not highlighting lines that are too long (thousands of characters).
    ///
    /// For efficiency reasons this returns only the changes to the current scope at each point in the line.
    /// You can use [`ScopeStack::apply`] on each operation in succession to get the stack for a given point.
    /// Look at the code in `highlighter.rs` for an example of doing this for highlighting purposes.
    ///
    /// The returned vector is in order both by index to apply at (the `usize`) and also by order to apply them at a
    /// given index (e.g popping old scopes before pushing new scopes).
    ///
    /// The [`SyntaxSet`] has to be the one that contained the syntax that was used to construct
    /// this [`ParseState`], or an extended version of it. Otherwise the parsing would return the
    /// wrong result or even panic. The reason for this is that contexts within the [`SyntaxSet`]
    /// are referenced via indexes.
    ///
    /// [`ScopeStack::apply`]: struct.ScopeStack.html#method.apply
    /// [`SyntaxSet`]: struct.SyntaxSet.html
    /// [`ParseState`]: struct.ParseState.html
    pub fn parse_line(
        &mut self,
        line: &str,
        syntax_set: &SyntaxSet,
    ) -> Result<Vec<(usize, ScopeStackOp)>, ParsingError> {
        if self.stack.is_empty() {
            return Err(ParsingError::MissingMainContext);
        }
        let mut match_start = 0;
        let mut res = Vec::new();

        if self.first_line {
            let cur_level = &self.stack[self.stack.len() - 1];
            let context = syntax_set.get_context(&cur_level.context)?;
            if !context.meta_content_scope.is_empty() {
                res.push((0, ScopeStackOp::Push(context.meta_content_scope[0])));
            }
            self.first_line = false;
        }

        let mut regions = Region::new();
        let fnv = BuildHasherDefault::<FnvHasher>::default();
        let mut search_cache: SearchCache = HashMap::with_capacity_and_hasher(128, fnv);
        // Used for detecting loops with push/pop, see long comment above.
        let mut non_consuming_push_at = (0, 0);

        while self.parse_next_token(
            line,
            syntax_set,
            &mut match_start,
            &mut search_cache,
            &mut regions,
            &mut non_consuming_push_at,
            &mut res,
        )? {}

        Ok(res)
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_next_token(
        &mut self,
        line: &str,
        syntax_set: &SyntaxSet,
        start: &mut usize,
        search_cache: &mut SearchCache,
        regions: &mut Region,
        non_consuming_push_at: &mut (usize, usize),
        ops: &mut Vec<(usize, ScopeStackOp)>,
    ) -> Result<bool, ParsingError> {
        let check_pop_loop = {
            let (pos, stack_depth) = *non_consuming_push_at;
            pos == *start && stack_depth == self.stack.len()
        };

        // Trim proto_starts that are no longer valid
        while self
            .proto_starts
            .last()
            .map(|start| *start >= self.stack.len())
            .unwrap_or(false)
        {
            self.proto_starts.pop();
        }

        let best_match = self.find_best_match(
            line,
            *start,
            syntax_set,
            search_cache,
            regions,
            check_pop_loop,
        )?;

        if let Some(reg_match) = best_match {
            if reg_match.would_loop {
                // A push that doesn't consume anything (a regex that resulted
                // in an empty match at the current position) can not be
                // followed by a non-consuming pop. Otherwise we're back where
                // we started and would try the same sequence of matches again,
                // resulting in an infinite loop. In this case, Sublime Text
                // advances one character and tries again, thus preventing the
                // loop.

                // println!("pop_would_loop for match {:?}, start {}", reg_match, *start);

                // nth(1) gets the next character if there is one. Need to do
                // this instead of just += 1 because we have byte indices and
                // unicode characters can be more than 1 byte.
                if let Some((i, _)) = line[*start..].char_indices().nth(1) {
                    *start += i;
                    return Ok(true);
                } else {
                    // End of line, no character to advance and no point trying
                    // any more patterns.
                    return Ok(false);
                }
            }

            let match_end = reg_match.regions.pos(0).unwrap().1;

            let consuming = match_end > *start;
            if !consuming {
                // The match doesn't consume any characters. If this is a
                // "push", remember the position and stack size so that we can
                // check the next "pop" for loops. Otherwise leave the state,
                // e.g. non-consuming "set" could also result in a loop.
                let context = reg_match.context;
                let match_pattern = context.match_at(reg_match.pat_index)?;
                if let MatchOperation::Push(_) = match_pattern.operation {
                    *non_consuming_push_at = (match_end, self.stack.len() + 1);
                }
            }

            *start = match_end;

            // ignore `with_prototype`s below this if a context is pushed
            if reg_match.from_with_prototype {
                // use current height, since we're before the actual push
                self.proto_starts.push(self.stack.len());
            }

            let level_context = {
                let id = &self.stack[self.stack.len() - 1].context;
                syntax_set.get_context(id)?
            };
            self.exec_pattern(line, &reg_match, level_context, syntax_set, ops)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn find_best_match<'a>(
        &self,
        line: &str,
        start: usize,
        syntax_set: &'a SyntaxSet,
        search_cache: &mut SearchCache,
        regions: &mut Region,
        check_pop_loop: bool,
    ) -> Result<Option<RegexMatch<'a>>, ParsingError> {
        let cur_level = &self.stack[self.stack.len() - 1];
        let context = syntax_set.get_context(&cur_level.context)?;
        let prototype = if let Some(ref p) = context.prototype {
            Some(p)
        } else {
            None
        };

        // Build an iterator for the contexts we want to visit in order
        let context_chain = {
            let proto_start = self.proto_starts.last().cloned().unwrap_or(0);
            // Sublime applies with_prototypes from bottom to top
            let with_prototypes = self.stack[proto_start..].iter().flat_map(|lvl| {
                lvl.prototypes
                    .iter()
                    .map(move |ctx| (true, ctx, lvl.captures.as_ref()))
            });
            let cur_prototype = prototype.into_iter().map(|ctx| (false, ctx, None));
            let cur_context =
                Some((false, &cur_level.context, cur_level.captures.as_ref())).into_iter();
            with_prototypes.chain(cur_prototype).chain(cur_context)
        };

        // println!("{:#?}", cur_level);
        // println!("token at {} on {}", start, line.trim_right());

        let mut min_start = usize::MAX;
        let mut best_match: Option<RegexMatch<'_>> = None;
        let mut pop_would_loop = false;

        for (from_with_proto, ctx, captures) in context_chain {
            for (pat_context, pat_index) in context_iter(syntax_set, syntax_set.get_context(ctx)?) {
                let match_pat = pat_context.match_at(pat_index)?;

                if let Some(match_region) =
                    self.search(line, start, match_pat, captures, search_cache, regions)
                {
                    let (match_start, match_end) = match_region.pos(0).unwrap();

                    // println!("matched pattern {:?} at start {} end {} (pop would loop: {}, min start: {}, initial start: {}, check_pop_loop: {}, stack_len: {})", match_pat, match_start, match_end, pop_would_loop, min_start, start, check_pop_loop, self.stack.len());

                    if match_start < min_start || (match_start == min_start && pop_would_loop) {
                        // New match is earlier in text than old match,
                        // or old match was a looping pop at the same
                        // position.

                        // println!("setting as current match");

                        min_start = match_start;

                        let consuming = match_end > start;
                        pop_would_loop = check_pop_loop
                            && !consuming
                            && matches!(match_pat.operation, MatchOperation::Pop);

                        let push_too_deep = matches!(match_pat.operation, MatchOperation::Push(_))
                            && self.stack.len() >= 100;

                        if push_too_deep {
                            return Ok(None);
                        }

                        best_match = Some(RegexMatch {
                            regions: match_region,
                            context: pat_context,
                            pat_index,
                            from_with_prototype: from_with_proto,
                            would_loop: pop_would_loop,
                        });

                        if match_start == start && !pop_would_loop {
                            // We're not gonna find a better match after this,
                            // so as an optimization we can stop matching now.
                            return Ok(best_match);
                        }
                    }
                }
            }
        }
        Ok(best_match)
    }

    fn search(
        &self,
        line: &str,
        start: usize,
        match_pat: &MatchPattern,
        captures: Option<&(Region, String)>,
        search_cache: &mut SearchCache,
        regions: &mut Region,
    ) -> Option<Region> {
        // println!("{} - {:?} - {:?}", match_pat.regex_str, match_pat.has_captures, cur_level.captures.is_some());
        let match_ptr = match_pat as *const MatchPattern;

        if let Some(maybe_region) = search_cache.get(&match_ptr) {
            if let Some(ref region) = *maybe_region {
                let match_start = region.pos(0).unwrap().0;
                if match_start >= start {
                    // Cached match is valid, return it. Otherwise do another
                    // search below.
                    return Some(region.clone());
                }
            } else {
                // Didn't find a match earlier, so no point trying to match it again
                return None;
            }
        }

        let (regex, can_cache) = match (match_pat.has_captures, captures) {
            (true, Some(captures)) => {
                let (region, s) = captures;
                (&match_pat.regex_with_refs(region, s), false)
            }
            _ => (match_pat.regex(), true),
        };
        // print!("  executing regex: {:?} at pos {} on line {}", regex.regex_str(), start, line);
        let matched = regex.search(line, start, line.len(), Some(regions));

        if matched {
            let (match_start, match_end) = regions.pos(0).unwrap();
            // this is necessary to avoid infinite looping on dumb patterns
            let does_something = match match_pat.operation {
                MatchOperation::None => match_start != match_end,
                MatchOperation::Push(_) => self.stack.len() < 100,
                _ => true,
            };
            if can_cache && does_something {
                search_cache.insert(match_pat, Some(regions.clone()));
            }
            if does_something {
                // print!("catch {} at {} on {}", match_pat.regex_str, match_start, line);
                return Some(regions.clone());
            }
        } else if can_cache {
            search_cache.insert(match_pat, None);
        }
        None
    }

    /// Returns true if the stack was changed
    fn exec_pattern<'a>(
        &mut self,
        line: &str,
        reg_match: &RegexMatch<'a>,
        level_context: &'a Context,
        syntax_set: &'a SyntaxSet,
        ops: &mut Vec<(usize, ScopeStackOp)>,
    ) -> Result<bool, ParsingError> {
        let (match_start, match_end) = reg_match.regions.pos(0).unwrap();
        let context = reg_match.context;
        let pat = context.match_at(reg_match.pat_index)?;
        // println!("running pattern {:?} on '{}' at {}, operation {:?}", pat.regex_str, line, match_start, pat.operation);

        self.push_meta_ops(
            true,
            match_start,
            level_context,
            &pat.operation,
            syntax_set,
            ops,
        )?;
        for s in &pat.scope {
            // println!("pushing {:?} at {}", s, match_start);
            ops.push((match_start, ScopeStackOp::Push(*s)));
        }
        if let Some(ref capture_map) = pat.captures {
            // captures could appear in an arbitrary order, have to produce ops in right order
            // ex: ((bob)|(hi))* could match hibob in wrong order, and outer has to push first
            // we don't have to handle a capture matching multiple times, Sublime doesn't
            let mut map: Vec<((usize, i32), ScopeStackOp)> = Vec::new();
            for &(cap_index, ref scopes) in capture_map.iter() {
                if let Some((cap_start, cap_end)) = reg_match.regions.pos(cap_index) {
                    // marking up empty captures causes pops to be sorted wrong
                    if cap_start == cap_end {
                        continue;
                    }
                    // println!("capture {:?} at {:?}-{:?}", scopes[0], cap_start, cap_end);
                    for scope in scopes.iter() {
                        map.push((
                            (cap_start, -((cap_end - cap_start) as i32)),
                            ScopeStackOp::Push(*scope),
                        ));
                    }
                    map.push(((cap_end, i32::MIN), ScopeStackOp::Pop(scopes.len())));
                }
            }
            map.sort_by(|a, b| a.0.cmp(&b.0));
            for ((index, _), op) in map.into_iter() {
                ops.push((index, op));
            }
        }
        if !pat.scope.is_empty() {
            // println!("popping at {}", match_end);
            ops.push((match_end, ScopeStackOp::Pop(pat.scope.len())));
        }
        self.push_meta_ops(
            false,
            match_end,
            level_context,
            &pat.operation,
            syntax_set,
            ops,
        )?;

        self.perform_op(line, &reg_match.regions, pat, syntax_set)
    }

    fn push_meta_ops(
        &self,
        initial: bool,
        index: usize,
        cur_context: &Context,
        match_op: &MatchOperation,
        syntax_set: &SyntaxSet,
        ops: &mut Vec<(usize, ScopeStackOp)>,
    ) -> Result<(), ParsingError> {
        // println!("metas ops for {:?}, initial: {}",
        //          match_op,
        //          initial);
        // println!("{:?}", cur_context.meta_scope);
        match *match_op {
            MatchOperation::Pop => {
                let v = if initial {
                    &cur_context.meta_content_scope
                } else {
                    &cur_context.meta_scope
                };
                if !v.is_empty() {
                    ops.push((index, ScopeStackOp::Pop(v.len())));
                }

                // cleared scopes are restored after the scopes from match pattern that invoked the pop are applied
                if !initial && cur_context.clear_scopes.is_some() {
                    ops.push((index, ScopeStackOp::Restore))
                }
            }
            // for some reason the ST3 behaviour of set is convoluted and is inconsistent with the docs and other ops
            // - the meta_content_scope of the current context is applied to the matched thing, unlike pop
            // - the clear_scopes are applied after the matched token, unlike push
            // - the interaction with meta scopes means that the token has the meta scopes of both the current scope and the new scope.
            MatchOperation::Push(ref context_refs) | MatchOperation::Set(ref context_refs) => {
                let is_set = matches!(*match_op, MatchOperation::Set(_));
                // a match pattern that "set"s keeps the meta_content_scope and meta_scope from the previous context
                if initial {
                    if is_set && cur_context.clear_scopes.is_some() {
                        // cleared scopes from the old context are restored immediately
                        ops.push((index, ScopeStackOp::Restore));
                    }
                    // add each context's meta scope
                    for r in context_refs.iter() {
                        let ctx = r.resolve(syntax_set)?;

                        if !is_set {
                            if let Some(clear_amount) = ctx.clear_scopes {
                                ops.push((index, ScopeStackOp::Clear(clear_amount)));
                            }
                        }

                        for scope in ctx.meta_scope.iter() {
                            ops.push((index, ScopeStackOp::Push(*scope)));
                        }
                    }
                } else {
                    let repush = (is_set
                        && (!cur_context.meta_scope.is_empty()
                            || !cur_context.meta_content_scope.is_empty()))
                        || context_refs.iter().any(|r| {
                            let ctx = r.resolve(syntax_set).unwrap();

                            !ctx.meta_content_scope.is_empty()
                                || (ctx.clear_scopes.is_some() && is_set)
                        });
                    if repush {
                        // remove previously pushed meta scopes, so that meta content scopes will be applied in the correct order
                        let mut num_to_pop: usize = context_refs
                            .iter()
                            .map(|r| {
                                let ctx = r.resolve(syntax_set).unwrap();
                                ctx.meta_scope.len()
                            })
                            .sum();

                        // also pop off the original context's meta scopes
                        if is_set {
                            num_to_pop +=
                                cur_context.meta_content_scope.len() + cur_context.meta_scope.len();
                        }

                        // do all the popping as one operation
                        if num_to_pop > 0 {
                            ops.push((index, ScopeStackOp::Pop(num_to_pop)));
                        }

                        // now we push meta scope and meta context scope for each context pushed
                        for r in context_refs {
                            let ctx = r.resolve(syntax_set)?;

                            // for some reason, contrary to my reading of the docs, set does this after the token
                            if is_set {
                                if let Some(clear_amount) = ctx.clear_scopes {
                                    ops.push((index, ScopeStackOp::Clear(clear_amount)));
                                }
                            }

                            for scope in ctx.meta_scope.iter() {
                                ops.push((index, ScopeStackOp::Push(*scope)));
                            }
                            for scope in ctx.meta_content_scope.iter() {
                                ops.push((index, ScopeStackOp::Push(*scope)));
                            }
                        }
                    }
                }
            }
            MatchOperation::None => (),
        }

        Ok(())
    }

    /// Returns true if the stack was changed
    fn perform_op(
        &mut self,
        line: &str,
        regions: &Region,
        pat: &MatchPattern,
        syntax_set: &SyntaxSet,
    ) -> Result<bool, ParsingError> {
        let (ctx_refs, old_proto_ids) = match pat.operation {
            MatchOperation::Push(ref ctx_refs) => (ctx_refs, None),
            MatchOperation::Set(ref ctx_refs) => {
                // a `with_prototype` stays active when the context is `set`
                // until the context layer in the stack (where the `with_prototype`
                // was initially applied) is popped off.
                (ctx_refs, self.stack.pop().map(|s| s.prototypes))
            }
            MatchOperation::Pop => {
                self.stack.pop();
                return Ok(true);
            }
            MatchOperation::None => return Ok(false),
        };
        for (i, r) in ctx_refs.iter().enumerate() {
            let mut proto_ids = if i == 0 {
                // it is only necessary to preserve the old prototypes
                // at the first stack frame pushed
                old_proto_ids.clone().unwrap_or_else(Vec::new)
            } else {
                Vec::new()
            };
            if i == ctx_refs.len() - 1 {
                // if a with_prototype was specified, and multiple contexts were pushed,
                // then the with_prototype applies only to the last context pushed, i.e.
                // top most on the stack after all the contexts are pushed - this is also
                // referred to as the "target" of the push by sublimehq - see
                // https://forum.sublimetext.com/t/dev-build-3111/19240/17 for more info
                if let Some(ref p) = pat.with_prototype {
                    proto_ids.push(p.id()?);
                }
            }
            let context_id = r.id()?;
            let context = syntax_set.get_context(&context_id)?;
            let captures = {
                let mut uses_backrefs = context.uses_backrefs;
                if !proto_ids.is_empty() {
                    uses_backrefs = uses_backrefs
                        || proto_ids
                            .iter()
                            .any(|id| syntax_set.get_context(id).unwrap().uses_backrefs);
                }
                if uses_backrefs {
                    Some((regions.clone(), line.to_owned()))
                } else {
                    None
                }
            };
            self.stack.push(StateLevel {
                context: context_id,
                prototypes: proto_ids,
                captures,
            });
        }
        Ok(true)
    }
}
