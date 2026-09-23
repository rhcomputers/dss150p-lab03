# Goal 1 — Why `.venv/` Is Not Committed

The virtual environment directory `.venv/` is deliberately excluded from Git for four reasons:

1. **It is platform-specific.** A venv created on Windows contains compiled binaries
   (`.exe`, `.pyd`) for Windows only. Those binaries — including the C extensions used by
   NumPy, pandas, and pyarrow — will not run on macOS or Linux, and may not even run on a
   different Windows machine with a different CPU architecture or Python patch version.

2. **It is large and noisy.** A typical venv contains hundreds of megabytes and thousands of
   files of third-party code that we did not author and do not want appearing in diffs or
   pull requests.

3. **Reproducibility comes from `requirements.txt`, not the folder.** The lab requires another
   machine to reproduce the environment. That is achieved by pinning exact package versions
   in `requirements.txt` and running `pip install -r requirements.txt` inside a fresh venv.
   We verified this works in Goal 1 Task A.

4. **Tracking it would contradict `.gitignore` and risk leaking secrets.** `.venv/` is already
   listed in `.gitignore`. Committing it would also risk capturing local paths, bytecode
   caches, and accidentally reading credentials from the environment.