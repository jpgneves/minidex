use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ignore::{ParallelVisitor, ParallelVisitorBuilder, WalkBuilder, WalkState};
use minidex::{FilesystemEntry, Index, Kind, SearchOptions, SearchResult, VolumeType, category};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(PartialEq)]
enum InputMode {
    Search,
    Target,
}

struct App {
    index: Arc<Index>,
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
}

impl App {
    fn new(index_path: &str, index_target: String) -> Result<Self> {
        let index = Arc::new(Index::open(index_path)?);
        let target_len = index_target.len();
        let mut app = App {
            index,
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
        };
        app.update_search();
        Ok(app)
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
        }
    }

    fn clamp_cursor(&self, new_cursor_pos: usize, max_len: usize) -> usize {
        new_cursor_pos.clamp(0, max_len)
    }

    fn update_search(&mut self) {
        let start = std::time::Instant::now();
        let options = SearchOptions::default();
        if self.input.is_empty() {
            let five_days_ago = std::time::SystemTime::now()
                .checked_sub(Duration::from_secs(5 * 24 * 60 * 60))
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;

            match self.index.recent_files(five_days_ago, 100, 0, options) {
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

        match self.index.search(&self.input, 100, 0, options) {
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
            let _ = self.index.delete(&res.path);
            self.update_search();
        }
    }

    fn compact(&self) {
        let index = Arc::clone(&self.index);
        std::thread::spawn(move || {
            let _ = index.force_compact_all();
        });
    }

    fn start_indexing(&self, path: String) {
        if self.indexing.swap(true, Ordering::SeqCst) {
            return; // Already indexing
        }

        let index = Arc::clone(&self.index);
        let indexing = Arc::clone(&self.indexing);
        let count = Arc::clone(&self.indexed_count);
        let duration = Arc::clone(&self.indexing_duration_ms);
        let last_files = Arc::clone(&self.last_indexing_files);

        count.store(0, Ordering::SeqCst);
        duration.store(0, Ordering::SeqCst);

        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            let mut builder = WalkBuilder::new(path);
            let walk = builder
                .threads(4)
                .hidden(false)
                .ignore(true)
                .git_ignore(true)
                .build_parallel();

            let mut scanner = Scanner {
                index: &index,
                file_count: count.clone(),
            };
            walk.visit(&mut scanner);

            let final_count = count.load(Ordering::SeqCst);
            last_files.store(final_count, Ordering::SeqCst);
            duration.store(start.elapsed().as_millis() as u64, Ordering::SeqCst);
            indexing.store(false, Ordering::SeqCst);
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

struct Scanner<'a> {
    index: &'a Index,
    file_count: Arc<AtomicU64>,
}

impl<'s, 'a: 's> ParallelVisitorBuilder<'s> for Scanner<'a> {
    fn build(&mut self) -> Box<dyn ParallelVisitor + 's> {
        Box::new(Scanner {
            index: self.index,
            file_count: self.file_count.clone(),
        })
    }
}

impl<'a> ParallelVisitor for Scanner<'a> {
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

            let _ = self.index.insert(FilesystemEntry {
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
                (KeyCode::Esc, _) => return Ok(()),
                (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(()),
                (KeyCode::Tab, _) => {
                    app.input_mode = match app.input_mode {
                        InputMode::Search => InputMode::Target,
                        InputMode::Target => InputMode::Search,
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
                        // Use a temporary variable to hold the output,
                        // as we can't easily print after restore() here without returning it.
                        // For now just exit. In a real app we might want to return the path.
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
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(f.area());

    let (search_color, target_color) = match app.input_mode {
        InputMode::Search => (Color::Yellow, Color::DarkGray),
        InputMode::Target => (Color::DarkGray, Color::Yellow),
    };

    let search_title = match app.input_mode {
        InputMode::Search => "Search (Active)",
        InputMode::Target => "Search",
    };

    let target_title = match app.input_mode {
        InputMode::Search => "Target Directory",
        InputMode::Target => "Target Directory (Active)",
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
                    .map(|dt| dt.format("%y-%m-%d %H:%M").to_string())
                    .unwrap_or_else(|| "N/A".to_string());
            let accessed =
                chrono::DateTime::from_timestamp((res.last_accessed / 1_000_000) as i64, 0)
                    .map(|dt| dt.format("%y-%m-%d %H:%M").to_string())
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
                    format!("M:{} A:{} ", modified, accessed),
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
        let min = sorted.first().unwrap();
        let max = sorted.last().unwrap();
        let sum: u128 = sorted.iter().sum();
        let avg = sum / sorted.len() as u128;
        let p99_idx = (sorted.len() as f64 * 0.99).ceil() as usize - 1;
        let p99 = sorted.get(p99_idx).unwrap_or(max);

        format!(
            "Latency - min: {}, max: {}, avg: {}, p99: {}",
            format_us(*min),
            format_us(*max),
            format_us(avg),
            format_us(*p99)
        )
    };

    let indexing_text = if app.indexing.load(Ordering::SeqCst) {
        format!(
            "Indexing... {} files so far",
            app.indexed_count.load(Ordering::SeqCst)
        )
    } else {
        let last_dur = app.indexing_duration_ms.load(Ordering::SeqCst);
        let last_files = app.last_indexing_files.load(Ordering::SeqCst);
        if last_dur > 0 || last_files > 0 {
            let fps = if last_dur > 0 {
                (last_files as f64 / (last_dur as f64 / 1000.0)) as u64
            } else {
                0
            };
            format!(
                "Last Indexing - files: {}, took: {} ({} fps)",
                last_files,
                format_ms(last_dur),
                fps
            )
        } else {
            "Not indexed yet".to_string()
        }
    };

    let metrics_line = Paragraph::new(Line::from(vec![
        Span::styled(latency_text, Style::default().fg(Color::Cyan)),
        Span::raw(" | "),
        Span::styled(indexing_text, Style::default().fg(Color::Magenta)),
    ]));
    f.render_widget(metrics_line, chunks[2]);

    f.render_stateful_widget(results_list, chunks[3], &mut app.list_state);

    let current_selection = if let Some(i) = app.list_state.selected() {
        format!("{} / {}", i + 1, app.results.len())
    } else {
        "0 / 0".to_string()
    };

    let status_line = if app.indexing.load(Ordering::SeqCst) {
        format!(
            "INDEXING... {} files",
            app.indexed_count.load(Ordering::SeqCst)
        )
    } else {
        "READY".to_string()
    };

    let help_message = Paragraph::new(Line::from(vec![
        Span::raw("Esc: quit | ↑/↓: navigate | Enter: select | Tab: switch focus | "),
        Span::styled(
            "Ctrl+R/I: index target | ",
            Style::default().fg(Color::Magenta),
        ),
        Span::styled(
            "Ctrl+K: compact | Ctrl+D: delete | ",
            Style::default().fg(Color::Red),
        ),
        Span::styled(status_line, Style::default().fg(Color::Yellow)),
        Span::raw(" | "),
        Span::styled(current_selection, Style::default().fg(Color::Green)),
    ]));
    f.render_widget(help_message, chunks[4]);
}
