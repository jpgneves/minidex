use minidex_core::Index;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let index_dir = "./idx";

    let index = Index::open(index_dir)?;

    index.insert(FilesystemEntry { path: (), kind: (), last_modified: (), last_accessed: () }"Learning Rust.pdf")?;
    index.commit()?;
    index.delete("Learning Rust.pdf")?;
    index.insert("Learning Rust.pdf")?;
    index.insert("Learning Rust.pdf")?;

    let results = index.search("Le Ru")?;
    assert_eq!(results.len(), 1);

    Ok(())
}
