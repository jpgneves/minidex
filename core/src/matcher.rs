use fst::Automaton;
use regex_automata::dfa::{dense, Automaton as _};
use regex_automata::util::primitives::StateID;
use regex_automata::Input;

pub(crate) struct RegexMatcher {
    dfa: dense::DFA<Vec<u32>>,
}

impl RegexMatcher {
    pub fn new(pattern: &str) -> Result<Self, regex_automata::dfa::dense::BuildError> {
        let config = dense::Config::new().start_kind(regex_automata::dfa::StartKind::Anchored);

        let dfa = dense::Builder::new().configure(config).build(pattern)?;
        Ok(Self { dfa })
    }

    pub fn is_match(&self, text: &str) -> bool {
        // Input::new wraps the byte slice.
        // The DFA engine processes the UTF-8 bytes directly.
        let input = Input::new(text.as_bytes());

        // try_search_fwd returns Ok(Some(Match)) if found
        self.dfa
            .try_search_fwd(&input)
            .map_or(false, |m| m.is_some())
    }
}

impl Automaton for RegexMatcher {
    type State = StateID;

    fn start(&self) -> Self::State {
        self.dfa
            .start_state_forward(
                &regex_automata::Input::new(b"").anchored(regex_automata::Anchored::Yes),
            )
            .expect("failed to get start state")
    }

    fn is_match(&self, state: &Self::State) -> bool {
        self.dfa.is_match_state(*state)
    }

    fn can_match(&self, state: &Self::State) -> bool {
        !self.dfa.is_dead_state(*state)
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.dfa.next_state(*state, byte)
    }
}
