use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ignore::{ParallelVisitor, ParallelVisitorBuilder, WalkBuilder, WalkState};
use minidex::{
    CompactorConfig, CompactorConfigBuilder, FilesystemEntry, Index, Kind, SearchOptions,
    SearchResult, VolumeType, category,
};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Bar, BarChart, BarGroup, Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs,
    },
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(PartialEq, Clone, Copy)]
enum Screen {
    Search,
    Stats,
    Config,
}

#[derive(PartialEq)]
enum InputMode {
    Search,
    Target,
    Config,
    OpenIndex,
}

struct IndexingGuard {
    indexing: Arc<AtomicBool>,
}

impl Drop for IndexingGuard {
    fn drop(&mut self) {
        self.indexing.store(false, Ordering::SeqCst);
    }
}

struct App {
    index_path: String,
    index: Option<Arc<Index>>,
    screen: Screen,
    input: String,
    cursor_position: usize,
    results: Vec<SearchResult>,
    list_state: ListState,
    indexing: Arc<AtomicBool>,
    indexed_count: Arc<AtomicU64>,
    indexing_duration_ms: Arc<AtomicU64>,
    last_indexing_files: Arc<AtomicU64>,
    index_target: String,
    target_cursor_position: usize,
    input_mode: InputMode,
    search_latencies_us: Vec<u128>,

    // Config screen state
    compactor_config: CompactorConfig,
    config_selection: usize,
    edit_min_merge: String,
    edit_flush_threshold: String,
    edit_tombstone_threshold: String,
    use_batch_insert: bool,
    edit_batch_size: String,

    // Last run stats
    last_indexing_mode: Arc<AtomicBool>, // true = batch
    last_indexing_batch_size: Arc<AtomicU64>,

    // Open Index state
    edit_index_path: String,
}

impl App {
    fn new(index_path: &str, index_target: String) -> Result<Self> {
        let config = CompactorConfig::default();
        let abs_index_path = std::path::Path::new(index_path)
            .canonicalize()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| index_path.to_string());

        let index_exists = std::path::Path::new(&abs_index_path).exists();
        let index = if index_exists {
            Some(Arc::new(Index::open_with_config(&abs_index_path, config)?))
        } else {
            None
        };
        let target_len = index_target.len();
        let mut app = App {
            index_path: abs_index_path.clone(),
            index,
            screen: Screen::Search,
            input: String::new(),
            cursor_position: 0,
            results: Vec::new(),
            list_state: ListState::default(),
            indexing: Arc::new(AtomicBool::new(false)),
            indexed_count: Arc::new(AtomicU64::new(0)),
            indexing_duration_ms: Arc::new(AtomicU64::new(0)),
            last_indexing_files: Arc::new(AtomicU64::new(0)),
            index_target,
            target_cursor_position: target_len,
            input_mode: InputMode::Search,
            search_latencies_us: Vec::new(),

            compactor_config: config,
            config_selection: 0,
            edit_flush_threshold: config.flush_threshold.to_string(),
            edit_min_merge: config.min_merge_count.to_string(),
            edit_tombstone_threshold: config.tombstone_threshold.to_string(),
            use_batch_insert: true,
            edit_batch_size: "10000".to_string(),

            last_indexing_mode: Arc::new(AtomicBool::new(true)),
            last_indexing_batch_size: Arc::new(AtomicU64::new(0)),

            edit_index_path: abs_index_path,
        };
        app.update_search();
        Ok(app)
    }

    fn get_index_or_init(&mut self) -> Result<Arc<Index>> {
        if let Some(ref idx) = self.index {
            return Ok(Arc::clone(idx));
        }

        let config = CompactorConfigBuilder::new()
            .flush_threshold(self.edit_flush_threshold.parse().unwrap_or(100000))
            .min_merge_count(self.edit_min_merge.parse().unwrap_or(8))
            .tombstone_threshold(self.edit_tombstone_threshold.parse().unwrap_or(2500))
            .build();

        let index = Arc::new(Index::open_with_config(&self.index_path, config)?);
        self.index = Some(Arc::clone(&index));
        Ok(index)
    }

    fn reopen_index(&mut self) -> Result<()> {
        let flush_threshold = self.edit_flush_threshold.parse().unwrap_or(100000);
        let min_merge = self.edit_min_merge.parse().unwrap_or(8);
        let tombstone_threshold = self.edit_tombstone_threshold.parse().unwrap_or(2500);

        let config = CompactorConfigBuilder::new()
            .flush_threshold(flush_threshold)
            .min_merge_count(min_merge)
            .tombstone_threshold(tombstone_threshold)
            .build();

        self.compactor_config = config;

        // Try to canonicalize the path if it changed
        let abs_path = std::path::Path::new(&self.edit_index_path)
            .canonicalize()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| self.edit_index_path.clone());

        self.index_path = abs_path;
        self.edit_index_path = self.index_path.clone();

        // Index handles its own drop (sync + join threads)
        let index = Arc::new(Index::open_with_config(&self.index_path, config)?);
        self.index = Some(Arc::clone(&index));
        self.results.clear();
        self.search_latencies_us.clear();
        self.update_search();
        Ok(())
    }

    fn change_index_path(&mut self) -> Result<()> {
        self.reopen_index()
    }

    fn move_cursor_left(&mut self) {
        match self.input_mode {
            InputMode::Search => {
                let cursor_moved_left = self.cursor_position.saturating_sub(1);
                self.cursor_position = self.clamp_cursor(cursor_moved_left, self.input.len());
            }
            InputMode::Target => {
                let cursor_moved_left = self.target_cursor_position.saturating_sub(1);
                self.target_cursor_position =
                    self.clamp_cursor(cursor_moved_left, self.index_target.len());
            }
            InputMode::Config => {
                self.target_cursor_position = self.target_cursor_position.saturating_sub(1);
            }
            InputMode::OpenIndex => {
                self.target_cursor_position = self.target_cursor_position.saturating_sub(1);
            }
        }
    }

    fn move_cursor_right(&mut self) {
        match self.input_mode {
            InputMode::Search => {
                let cursor_moved_right = self.cursor_position.saturating_add(1);
                self.cursor_position = self.clamp_cursor(cursor_moved_right, self.input.len());
            }
            InputMode::Target => {
                let cursor_moved_right = self.target_cursor_position.saturating_add(1);
                self.target_cursor_position =
                    self.clamp_cursor(cursor_moved_right, self.index_target.len());
            }
            InputMode::Config => {
                let len = match self.config_selection {
                    0 => self.edit_flush_threshold.len(),
                    1 => self.edit_min_merge.len(),
                    2 => self.edit_tombstone_threshold.len(),
                    3 => 0, // Toggle field
                    _ => self.edit_batch_size.len(),
                };
                self.target_cursor_position = (self.target_cursor_position + 1).min(len);
            }
            InputMode::OpenIndex => {
                self.target_cursor_position =
                    (self.target_cursor_position + 1).min(self.edit_index_path.len());
            }
        }
    }

    fn enter_char(&mut self, new_char: char) {
        match self.input_mode {
            InputMode::Search => {
                self.input.insert(self.cursor_position, new_char);
                self.move_cursor_right();
                self.update_search();
            }
            InputMode::Target => {
                self.index_target
                    .insert(self.target_cursor_position, new_char);
                self.move_cursor_right();
            }
            InputMode::Config => {
                if new_char.is_ascii_digit() {
                    let target_pos = self.target_cursor_position;
                    let current_val = match self.config_selection {
                        0 => &mut self.edit_flush_threshold,
                        1 => &mut self.edit_min_merge,
                        2 => &mut self.edit_tombstone_threshold,
                        3 => return, // Toggle field
                        _ => &mut self.edit_batch_size,
                    };
                    current_val.insert(target_pos, new_char);
                    self.move_cursor_right();
                }
            }
            InputMode::OpenIndex => {
                self.edit_index_path
                    .insert(self.target_cursor_position, new_char);
                self.move_cursor_right();
            }
        }
    }

    fn delete_char(&mut self) {
        match self.input_mode {
            InputMode::Search => {
                if self.cursor_position != 0 {
                    let left_to_left = self.input.chars().take(self.cursor_position - 1);
                    let right_to_left = self.input.chars().skip(self.cursor_position);
                    self.input = left_to_left.chain(right_to_left).collect();
                    self.move_cursor_left();
                    self.update_search();
                }
            }
            InputMode::Target => {
                if self.target_cursor_position != 0 {
                    let left_to_left = self
                        .index_target
                        .chars()
                        .take(self.target_cursor_position - 1);
                    let right_to_left = self.index_target.chars().skip(self.target_cursor_position);
                    self.index_target = left_to_left.chain(right_to_left).collect();
                    self.move_cursor_left();
                }
            }
            InputMode::Config => {
                if self.target_cursor_position != 0 {
                    let target_pos = self.target_cursor_position;
                    let current_val = match self.config_selection {
                        0 => &mut self.edit_flush_threshold,
                        1 => &mut self.edit_min_merge,
                        2 => &mut self.edit_tombstone_threshold,
                        3 => return, // Toggle field
                        _ => &mut self.edit_batch_size,
                    };
                    let left_to_left = current_val.chars().take(target_pos - 1);
                    let right_to_left = current_val.chars().skip(target_pos);
                    *current_val = left_to_left.chain(right_to_left).collect();
                    self.move_cursor_left();
                }
            }
            InputMode::OpenIndex => {
                if self.target_cursor_position != 0 {
                    let left_to_left = self
                        .edit_index_path
                        .chars()
                        .take(self.target_cursor_position - 1);
                    let right_to_left = self
                        .edit_index_path
                        .chars()
                        .skip(self.target_cursor_position);
                    self.edit_index_path = left_to_left.chain(right_to_left).collect();
                    self.move_cursor_left();
                }
            }
        }
    }

    fn get_config_value_mut(&mut self) -> &mut String {
        match self.config_selection {
            0 => &mut self.edit_flush_threshold,
            1 => &mut self.edit_min_merge,
            2 => &mut self.edit_tombstone_threshold,
            3 => panic!("toggle field has no string value"),
            _ => &mut self.edit_batch_size,
        }
    }

    fn clamp_cursor(&self, new_cursor_pos: usize, max_len: usize) -> usize {
        new_cursor_pos.clamp(0, max_len)
    }

    fn update_search(&mut self) {
        let index = match &self.index {
            Some(i) => i,
            None => {
                self.results.clear();
                self.list_state.select(None);
                return;
            }
        };

        let start = std::time::Instant::now();
        let options = SearchOptions::default();
        if self.input.is_empty() {
            let five_days_ago = std::time::SystemTime::now()
                .checked_sub(Duration::from_secs(5 * 24 * 60 * 60))
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            match index.recent_files(five_days_ago, 100, 0, options) {
                Ok(results) => {
                    self.results = results;
                    if !self.results.is_empty() {
                        self.list_state.select(Some(0));
                    } else {
                        self.list_state.select(None);
                    }
                }
                Err(_) => {
                    self.results.clear();
                    self.list_state.select(None);
                }
            }
            let elapsed = start.elapsed().as_micros();
            self.search_latencies_us.push(elapsed);
            return;
        }

        match index.search(&self.input, 100, 0, options) {
            Ok(results) => {
                self.results = results;
                if !self.results.is_empty() {
                    self.list_state.select(Some(0));
                } else {
                    self.list_state.select(None);
                }
            }
            Err(_) => {
                self.results.clear();
                self.list_state.select(None);
            }
        }
        let elapsed = start.elapsed().as_micros();
        self.search_latencies_us.push(elapsed);
    }

    fn next_result(&mut self) {
        if self.results.is_empty() {
            return;
        }
        let i = match self.list_state.selected() {
            Some(i) => {
                if i >= self.results.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.list_state.select(Some(i));
    }

    fn previous_result(&mut self) {
        if self.results.is_empty() {
            return;
        }
        let i = match self.list_state.selected() {
            Some(i) => {
                if i == 0 {
                    self.results.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.list_state.select(Some(i));
    }

    fn delete_selected(&mut self) {
        if let Some(i) = self.list_state.selected()
            && let Some(res) = self.results.get(i)
        {
            let path = res.path.clone();
            if let Ok(index) = self.get_index_or_init() {
                let _ = index.delete(&path);
                self.update_search();
            }
        }
    }

    fn compact(&mut self) {
        if let Ok(index) = self.get_index_or_init() {
            std::thread::spawn(move || {
                let _ = index.force_compact_all();
            });
        }
    }

    fn start_indexing(&mut self, path: String) {
        if self.indexing.swap(true, Ordering::SeqCst) {
            return; // Already indexing
        }

        let index = match self.get_index_or_init() {
            Ok(i) => i,
            Err(_) => {
                self.indexing.store(false, Ordering::SeqCst);
                return;
            }
        };

        let indexing = Arc::clone(&self.indexing);
        let count = Arc::clone(&self.indexed_count);
        let duration = Arc::clone(&self.indexing_duration_ms);
        let last_files = Arc::clone(&self.last_indexing_files);
        let last_mode = Arc::clone(&self.last_indexing_mode);
        let last_bs = Arc::clone(&self.last_indexing_batch_size);
        let use_batch = self.use_batch_insert;
        let batch_size = self.edit_batch_size.parse::<usize>().unwrap_or(10000);

        count.store(0, Ordering::SeqCst);

        std::thread::spawn(move || {
            let _guard = IndexingGuard { indexing };
            let (tx, rx) = std::sync::mpsc::channel();

            let tx_clone = tx.clone();
            let count_clone = count.clone();
            let path_clone = path.clone();

            // Spawn parallel walker
            std::thread::spawn(move || {
                let mut builder = WalkBuilder::new(path_clone);
                let walk = builder
                    .hidden(false)
                    .ignore(true)
                    .git_ignore(true)
                    .build_parallel();

                let mut scanner = ChannelScanner {
                    tx: tx_clone,
                    file_count: count_clone,
                };
                walk.visit(&mut scanner);
            });
            drop(tx); // Drop original sender so receiver terminates when walker finishes

            let start = std::time::Instant::now();
            if use_batch {
                let _ = index.insert_batch(rx, batch_size);
            } else {
                for entry in rx {
                    // We need a way to insert with tokens in 1-by-1 mode too for parity
                    // For now, insert_batch with size 1 would work, or we can just use insert
                    // since the bottleneck was mainly the batch lock hold time.
                    let _ = index.insert(entry);
                }
            }

            let final_count = count.load(Ordering::SeqCst);
            last_files.store(final_count, Ordering::SeqCst);
            duration.store(start.elapsed().as_millis() as u64, Ordering::SeqCst);
            last_mode.store(use_batch, Ordering::SeqCst);
            last_bs.store(batch_size as u64, Ordering::SeqCst);
        });
    }
}

fn detect_category(path: &std::path::Path) -> u8 {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_lowercase();

    match ext.as_str() {
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "svg" => category::IMAGE,
        "mp4" | "mkv" | "mov" | "avi" | "webm" => category::VIDEO,
        "mp3" | "wav" | "ogg" | "flac" | "aac" => category::AUDIO,
        "pdf" | "doc" | "docx" | "ppt" | "pptx" | "xls" | "xlsx" | "odt" => category::DOCUMENT,
        "zip" | "tar" | "gz" | "bz2" | "xz" | "7z" | "rar" => category::ARCHIVE,
        "txt" | "md" | "rs" | "js" | "ts" | "c" | "cpp" | "h" | "hpp" | "py" | "go" | "rb"
        | "json" | "yaml" | "toml" | "html" | "css" => category::TEXT,
        _ => category::OTHER,
    }
}

struct ChannelScanner {
    tx: std::sync::mpsc::Sender<FilesystemEntry>,
    file_count: Arc<AtomicU64>,
}

impl<'s> ParallelVisitorBuilder<'s> for ChannelScanner {
    fn build(&mut self) -> Box<dyn ParallelVisitor + 's> {
        Box::new(ChannelScanner {
            tx: self.tx.clone(),
            file_count: self.file_count.clone(),
        })
    }
}

impl ParallelVisitor for ChannelScanner {
    fn visit(&mut self, entry: Result<ignore::DirEntry, ignore::Error>) -> WalkState {
        if let Ok(entry) = entry {
            let Ok(metadata) = entry.metadata() else {
                return WalkState::Skip;
            };
            let kind = if metadata.is_dir() {
                Kind::Directory
            } else if metadata.is_symlink() {
                Kind::Symlink
            } else {
                Kind::File
            };
            let last_modified = metadata
                .modified()
                .unwrap_or(std::time::SystemTime::now())
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;

            let last_accessed = metadata
                .accessed()
                .unwrap_or(std::time::SystemTime::now())
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;

            let cat = detect_category(entry.path());

            let _ = self.tx.send(FilesystemEntry {
                path: entry.path().to_path_buf(),
                volume: "/".to_string(),
                volume_type: VolumeType::Local,
                kind,
                last_modified,
                last_accessed,
                category: cat,
            });
            self.file_count.fetch_add(1, Ordering::SeqCst);
            WalkState::Continue
        } else {
            WalkState::Skip
        }
    }
}

fn main() -> Result<()> {
    let index_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "index".to_string());

    let target_dir = std::env::args()
        .nth(2)
        .unwrap_or_else(|| std::env::var("HOME").unwrap_or_else(|_| ".".to_string()));

    let mut terminal = ratatui::init();
    let app_result = App::new(&index_path, target_dir).map(|app| run_app(&mut terminal, app));
    ratatui::restore();

    match app_result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(anyhow::anyhow!("IO error: {}", e)),
        Err(e) => Err(e),
    }
}

fn run_app(terminal: &mut DefaultTerminal, mut app: App) -> io::Result<()> {
    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match (key.code, key.modifiers) {
                (KeyCode::Esc, _) => {
                    if app.input_mode == InputMode::OpenIndex {
                        app.input_mode = InputMode::Search;
                    } else {
                        return Ok(());
                    }
                }
                (KeyCode::Char('q'), _)
                    if app.input_mode != InputMode::Search
                        && app.input_mode != InputMode::Target
                        && app.input_mode != InputMode::Config
                        && app.input_mode != InputMode::OpenIndex =>
                {
                    return Ok(());
                }
                (KeyCode::Char('o'), KeyModifiers::CONTROL) => {
                    app.input_mode = InputMode::OpenIndex;
                    app.target_cursor_position = app.edit_index_path.len();
                }
                (KeyCode::F(1), _) => {
                    app.screen = Screen::Search;
                    app.input_mode = InputMode::Search;
                }
                (KeyCode::F(2), _) => {
                    app.screen = Screen::Stats;
                }
                (KeyCode::F(3), _) => {
                    app.screen = Screen::Config;
                    app.input_mode = InputMode::Config;
                }
                _ => match app.input_mode {
                    InputMode::OpenIndex => match (key.code, key.modifiers) {
                        (KeyCode::Enter, _) => {
                            let _ = app.change_index_path();
                            app.input_mode = InputMode::Search;
                        }
                        (KeyCode::Char(c), _) => app.enter_char(c),
                        (KeyCode::Backspace, _) => app.delete_char(),
                        (KeyCode::Left, _) => app.move_cursor_left(),
                        (KeyCode::Right, _) => app.move_cursor_right(),
                        _ => {}
                    },
                    _ => match app.screen {
                        Screen::Search => match (key.code, key.modifiers) {
                            (KeyCode::Tab, _) => {
                                app.input_mode = match app.input_mode {
                                    InputMode::Search => InputMode::Target,
                                    InputMode::Target => InputMode::Search,
                                    _ => InputMode::Search,
                                };
                            }
                            (KeyCode::Char('i'), KeyModifiers::CONTROL)
                            | (KeyCode::Char('r'), KeyModifiers::CONTROL) => {
                                let target = app.index_target.clone();
                                app.start_indexing(target);
                            }
                            (KeyCode::Char('k'), KeyModifiers::CONTROL) => {
                                app.compact();
                            }
                            (KeyCode::Char('d'), KeyModifiers::CONTROL) => {
                                app.delete_selected();
                            }
                            (KeyCode::Enter, _) => {
                                if let Some(i) = app.list_state.selected()
                                    && let Some(_res) = app.results.get(i)
                                {
                                    return Ok(());
                                }
                            }
                            (KeyCode::Char(c), _) => app.enter_char(c),
                            (KeyCode::Backspace, _) => app.delete_char(),
                            (KeyCode::Left, _) => app.move_cursor_left(),
                            (KeyCode::Right, _) => app.move_cursor_right(),
                            (KeyCode::Up, _) => app.previous_result(),
                            (KeyCode::Down, _) => app.next_result(),
                            _ => {}
                        },
                        Screen::Stats => {
                            // Stats screen might not have much interaction yet
                        }
                        Screen::Config => match (key.code, key.modifiers) {
                            (KeyCode::Up, _) => {
                                app.config_selection = app.config_selection.saturating_sub(1);
                                app.target_cursor_position = match app.config_selection {
                                    3 => 0,
                                    _ => app.get_config_value_mut().len(),
                                };
                            }
                            (KeyCode::Down, _) => {
                                app.config_selection = (app.config_selection + 1).min(4);
                                app.target_cursor_position = match app.config_selection {
                                    3 => 0,
                                    _ => app.get_config_value_mut().len(),
                                };
                            }
                            (KeyCode::Char(' '), _) | (KeyCode::Enter, _)
                                if app.config_selection == 3 =>
                            {
                                app.use_batch_insert = !app.use_batch_insert;
                            }
                            (KeyCode::Enter, _) => {
                                let _ = app.reopen_index();
                            }
                            (KeyCode::Char(c), _) => app.enter_char(c),
                            (KeyCode::Backspace, _) => app.delete_char(),
                            (KeyCode::Left, _) => app.move_cursor_left(),
                            (KeyCode::Right, _) => app.move_cursor_right(),
                            _ => {}
                        },
                    },
                },
            }
        }
    }
}

fn format_us(us: u128) -> String {
    if us < 1000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.2}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn format_ms(ms: u64) -> String {
    if ms < 1000 {
        format!("{}ms", ms)
    } else if ms < 60_000 {
        format!("{:.2}s", ms as f64 / 1000.0)
    } else {
        format!("{:.2}m", ms as f64 / 60_000.0)
    }
}

fn ui(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(f.area());

    let titles = vec![" [F1] Search ", " [F2] Stats ", " [F3] Config "];
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Minidex (Index: {}) ", app.index_path)),
        )
        .select(match app.screen {
            Screen::Search => 0,
            Screen::Stats => 1,
            Screen::Config => 2,
        })
        .style(Style::default().fg(Color::Cyan))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[0]);

    match app.screen {
        Screen::Search => ui_search(f, app, chunks[1]),
        Screen::Stats => ui_stats(f, app, chunks[1]),
        Screen::Config => ui_config(f, app, chunks[1]),
    }

    ui_help(f, app, chunks[2]);

    if app.input_mode == InputMode::OpenIndex {
        let area = centered_rect(60, 10, f.area());
        f.render_widget(Clear, area); // clear the background
        let block = Block::default()
            .title(" Open Index at Path ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow));
        let p = Paragraph::new(app.edit_index_path.as_str())
            .block(block)
            .style(Style::default().fg(Color::Yellow));
        f.render_widget(p, area);

        f.set_cursor_position((area.x + app.target_cursor_position as u16 + 1, area.y + 1));
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn ui_search(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(area);

    let (search_color, target_color) = match app.input_mode {
        InputMode::Search => (Color::Yellow, Color::DarkGray),
        InputMode::Target => (Color::DarkGray, Color::Yellow),
        _ => (Color::DarkGray, Color::DarkGray),
    };

    let search_title = match app.input_mode {
        InputMode::Search => "Search (Active)",
        _ => "Search",
    };

    let target_title = match app.input_mode {
        InputMode::Target => "Target Directory (Active)",
        _ => "Target Directory",
    };

    let input = Paragraph::new(app.input.as_str())
        .style(Style::default().fg(search_color))
        .block(Block::default().borders(Borders::ALL).title(search_title));
    f.render_widget(input, chunks[0]);

    let target = Paragraph::new(app.index_target.as_str())
        .style(Style::default().fg(target_color))
        .block(Block::default().borders(Borders::ALL).title(target_title));
    f.render_widget(target, chunks[1]);

    match app.input_mode {
        InputMode::Search => {
            let cursor_x = chunks[0].x + app.cursor_position as u16 + 1;
            let cursor_y = chunks[0].y + 1;
            if cursor_x < chunks[0].x + chunks[0].width - 1 {
                f.set_cursor_position((cursor_x, cursor_y));
            }
        }
        InputMode::Target => {
            let cursor_x = chunks[1].x + app.target_cursor_position as u16 + 1;
            let cursor_y = chunks[1].y + 1;
            if cursor_x < chunks[1].x + chunks[1].width - 1 {
                f.set_cursor_position((cursor_x, cursor_y));
            }
        }
        _ => {}
    }

    let results: Vec<ListItem> = app
        .results
        .iter()
        .map(|res| {
            let kind_str = match res.kind {
                minidex::Kind::File => "FILE",
                minidex::Kind::Directory => "DIR ",
                minidex::Kind::Symlink => "SYM ",
            };

            let score_style = if res.score > 20.0 {
                Style::default().fg(Color::Green)
            } else if res.score > 10.0 {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::Gray)
            };

            let cat_str = match res.category {
                category::IMAGE => "IMG ",
                category::VIDEO => "VID ",
                category::AUDIO => "AUD ",
                category::DOCUMENT => "DOC ",
                category::ARCHIVE => "ARC ",
                category::TEXT => "TXT ",
                _ => "OTH ",
            };

            let modified =
                chrono::DateTime::from_timestamp((res.last_modified / 1_000_000) as i64, 0)
                    .map(|dt| dt.format("%y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "N/A".to_string());
            let accessed =
                chrono::DateTime::from_timestamp((res.last_accessed / 1_000_000) as i64, 0)
                    .map(|dt| dt.format("%y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "N/A".to_string());

            let content = Line::from(vec![
                Span::styled(
                    format!("{: <5} ", kind_str),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(
                    format!("{: <4} ", cat_str),
                    Style::default().fg(Color::Magenta),
                ),
                Span::styled(format!("[{:>5.1}] ", res.score), score_style),
                Span::styled(
                    format!("M:{} ", modified),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("A:{} ", accessed),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw(res.path.to_string_lossy()),
            ]);
            ListItem::new(content)
        })
        .collect();

    let title = if app.input.is_empty() {
        format!("Recent Files ({})", app.results.len())
    } else {
        format!("Results ({})", app.results.len())
    };

    let results_list = List::new(results)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");

    let latency_text = if app.search_latencies_us.is_empty() {
        "Latency: N/A".to_string()
    } else {
        let mut sorted = app.search_latencies_us.clone();
        sorted.sort_unstable();
        let avg = sorted.iter().sum::<u128>() / sorted.len() as u128;
        let p99 = sorted[((sorted.len() as f64 * 0.99) as usize).max(1) - 1];
        format!("Avg Latency: {} | P99: {}", format_us(avg), format_us(p99))
    };

    let indexing_text = if app.indexing.load(Ordering::SeqCst) {
        format!(
            "Indexing... {} files",
            app.indexed_count.load(Ordering::SeqCst)
        )
    } else {
        format!(
            "Last Indexing: {} files",
            app.last_indexing_files.load(Ordering::SeqCst)
        )
    };

    let metrics_line = Paragraph::new(Line::from(vec![
        Span::styled(latency_text, Style::default().fg(Color::Cyan)),
        Span::raw(" | "),
        Span::styled(indexing_text, Style::default().fg(Color::Magenta)),
    ]));
    f.render_widget(metrics_line, chunks[2]);

    f.render_stateful_widget(results_list, chunks[3], &mut app.list_state);
}

fn ui_stats(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(area);

    if app.search_latencies_us.is_empty() {
        f.render_widget(
            Paragraph::new("No search latency data available yet.")
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Latency Stats"),
                ),
            chunks[0],
        );
    } else {
        let mut sorted = app.search_latencies_us.clone();
        sorted.sort_unstable();
        let n = sorted.len();

        let p50 = sorted[n / 2];
        let p75 = sorted[((n as f64 * 0.75) as usize).max(1) - 1];
        let p90 = sorted[((n as f64 * 0.90) as usize).max(1) - 1];
        let p95 = sorted[((n as f64 * 0.95) as usize).max(1) - 1];
        let p99 = sorted[((n as f64 * 0.99) as usize).max(1) - 1];
        let max = sorted[n - 1];

        let data = [
            Bar::with_label("P50", p50 as u64).style(Style::default().fg(Color::Green)),
            Bar::with_label("P75", p75 as u64).style(Style::default().fg(Color::Cyan)),
            Bar::with_label("P90", p90 as u64).style(Style::default().fg(Color::Blue)),
            Bar::with_label("P95", p95 as u64).style(Style::default().fg(Color::Yellow)),
            Bar::with_label("P99", p99 as u64).style(Style::default().fg(Color::Red)),
            Bar::with_label("Max", max as u64).style(Style::default().fg(Color::Magenta)),
        ];

        let barchart = BarChart::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Search Latency Distribution (µs)"),
            )
            .data(BarGroup::default().bars(&data))
            .bar_width(10)
            .bar_gap(2)
            .value_style(Style::default().fg(Color::Black).bg(Color::Cyan));

        f.render_widget(barchart, chunks[0]);
    }

    // Indexing stats
    let last_dur = app.indexing_duration_ms.load(Ordering::SeqCst);
    let last_files = app.last_indexing_files.load(Ordering::SeqCst);
    let last_mode = app.last_indexing_mode.load(Ordering::SeqCst);
    let last_bs = app.last_indexing_batch_size.load(Ordering::SeqCst);
    let indexing_active = app.indexing.load(Ordering::SeqCst);
    let current_indexed_count = app.indexed_count.load(Ordering::SeqCst);

    let fps = if last_dur > 0 {
        (last_files as f64 / (last_dur as f64 / 1000.0)) as u64
    } else {
        0
    };

    let mut indexing_info = vec![Line::from(vec![
        Span::raw("Indexing Status:        "),
        Span::styled(
            if indexing_active { "ACTIVE" } else { "IDLE" },
            Style::default().fg(if indexing_active {
                Color::Yellow
            } else {
                Color::Green
            }),
        ),
    ])];

    if indexing_active {
        indexing_info.push(Line::from(vec![
            Span::raw("Current Progress:       "),
            Span::styled(
                format!("{} files", current_indexed_count),
                Style::default().fg(Color::Yellow),
            ),
        ]));
    }

    indexing_info.extend(vec![
        Line::from(vec![
            Span::raw("Last Indexing Duration: "),
            Span::styled(format_ms(last_dur), Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::raw("Last Indexing Files:    "),
            Span::styled(last_files.to_string(), Style::default().fg(Color::Magenta)),
        ]),
        Line::from(vec![
            Span::raw("Last Indexing Speed:    "),
            Span::styled(format!("{} fps", fps), Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::raw("Last Run - Batch Mode:  "),
            Span::styled(
                if last_mode {
                    format!("Enabled (bs={})", last_bs)
                } else {
                    "Disabled (1-by-1)".to_string()
                },
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::raw("Search Samples:         "),
            Span::styled(
                app.search_latencies_us.len().to_string(),
                Style::default().fg(Color::Green),
            ),
        ]),
        Line::from(vec![Span::raw("--- Compactor Settings ---")]),
        Line::from(vec![
            Span::raw("Min Merge Count:        "),
            Span::styled(
                app.compactor_config.min_merge_count.to_string(),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::raw("Flush Threshold:        "),
            Span::styled(
                app.compactor_config.flush_threshold.to_string(),
                Style::default().fg(Color::Magenta),
            ),
        ]),
        Line::from(vec![
            Span::raw("Tombstone Threshold:    "),
            Span::styled(
                app.compactor_config.tombstone_threshold.to_string(),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        Line::from(vec![Span::raw("--- Current Indexing Config ---")]),
        Line::from(vec![
            Span::raw("Batch Mode:             "),
            Span::styled(
                if app.use_batch_insert {
                    "Enabled"
                } else {
                    "Disabled (1-by-1)"
                },
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(vec![
            Span::raw("Batch Size:             "),
            Span::styled(
                app.edit_batch_size.clone(),
                Style::default().fg(Color::Magenta),
            ),
        ]),
    ]);

    let info_paragraph = Paragraph::new(indexing_info).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Indexing & General Stats"),
    );
    f.render_widget(info_paragraph, chunks[1]);
}

fn ui_config(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    let compactor_fields = [
        ("Flush Threshold", &app.edit_flush_threshold),
        ("Min Merge Count", &app.edit_min_merge),
        ("Tombstone Threshold", &app.edit_tombstone_threshold),
    ];

    for (i, (label, value)) in compactor_fields.iter().enumerate() {
        let style = if app.config_selection == i {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::Gray)
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .title(if app.config_selection == i {
                format!("{} (Active)", label)
            } else {
                label.to_string()
            });

        let p = Paragraph::new(value.as_str()).block(block).style(style);
        f.render_widget(p, chunks[i]);

        if app.config_selection == i && app.input_mode == InputMode::Config {
            let cursor_x = chunks[i].x + app.target_cursor_position as u16 + 1;
            let cursor_y = chunks[i].y + 1;
            if cursor_x < chunks[i].x + chunks[i].width - 1 {
                f.set_cursor_position((cursor_x, cursor_y));
            }
        }
    }

    // Batch Indexing Toggle
    let toggle_style = if app.config_selection == 3 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Gray)
    };
    let toggle_val = if app.use_batch_insert {
        "[X] Enabled"
    } else {
        "[ ] Disabled (1-by-1 Parallel)"
    };
    let toggle_p = Paragraph::new(toggle_val)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Batch Indexing Mode (Space to Toggle)"),
        )
        .style(toggle_style);
    f.render_widget(toggle_p, chunks[3]);

    // Batch Size
    let batch_size_style = if app.config_selection == 4 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Gray)
    };
    let batch_size_p = Paragraph::new(app.edit_batch_size.as_str())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(if app.config_selection == 4 {
                    "Batch Size (Active)"
                } else {
                    "Batch Size"
                }),
        )
        .style(batch_size_style);
    f.render_widget(batch_size_p, chunks[4]);

    if app.config_selection == 4 && app.input_mode == InputMode::Config {
        let cursor_x = chunks[4].x + app.target_cursor_position as u16 + 1;
        let cursor_y = chunks[4].y + 1;
        if cursor_x < chunks[4].x + chunks[4].width - 1 {
            f.set_cursor_position((cursor_x, cursor_y));
        }
    }

    let help_text = vec![
        Line::from("Use Up/Down to switch fields."),
        Line::from("Enter a new value (digits only)."),
        Line::from(Span::styled(
            "Changes to Compactor require Re-opening Index (Enter).",
            Style::default().fg(Color::Yellow),
        )),
        Line::from(Span::styled(
            "Batch settings apply to next Indexing session.",
            Style::default().fg(Color::Cyan),
        )),
    ];
    let help_p = Paragraph::new(help_text).block(Block::default().borders(Borders::NONE));
    f.render_widget(help_p, chunks[5]);
}

fn ui_help(f: &mut Frame, app: &App, area: Rect) {
    let current_selection = if app.screen == Screen::Search {
        if let Some(i) = app.list_state.selected() {
            format!("{} / {}", i + 1, app.results.len())
        } else {
            "0 / 0".to_string()
        }
    } else {
        "".to_string()
    };

    let status_line = if app.indexing.load(Ordering::SeqCst) {
        "INDEXING...".to_string()
    } else {
        "READY".to_string()
    };

    let help_msg = match app.input_mode {
        InputMode::OpenIndex => "Esc: Cancel | Enter: Open Path",
        _ => match app.screen {
            Screen::Search => {
                "Esc: Quit | Tab: Switch | Ctrl+O: Open | Ctrl+R: Index | Ctrl+K: Compact | Ctrl+D: Delete"
            }
            Screen::Stats => "Esc: Quit | F1-F3: Switch | Ctrl+O: Open",
            Screen::Config => "Esc: Quit | Up/Down: Navigate | Enter: Apply | Ctrl+O: Open",
        },
    };

    let help_line = Line::from(vec![
        Span::styled(help_msg, Style::default().fg(Color::DarkGray)),
        Span::raw(" | "),
        Span::styled(status_line, Style::default().fg(Color::Yellow)),
        Span::raw(" | "),
        Span::styled(current_selection, Style::default().fg(Color::Green)),
    ]);

    f.render_widget(Paragraph::new(help_line), area);
}
