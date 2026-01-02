use fst::Automaton;
use regex_automata::dfa::{dense, Automaton as _};
use regex_automata::util::primitives::StateID;

pub(crate) struct RegexMatcher {
    dfa: dense::DFA<Vec<u32>>,
}

impl RegexMatcher {
    pub fn new(pattern: &str) -> Result<Self, regex_automata::dfa::dense::BuildError> {
        let dfa = dense::DFA::new(pattern)?;
        Ok(Self { dfa })
    }
}

impl Automaton for RegexMatcher {
    type State = StateID;

    fn start(&self) -> Self::State {
        self.dfa
            .start_state_forward(&regex_automata::Input::new(&[]))
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
