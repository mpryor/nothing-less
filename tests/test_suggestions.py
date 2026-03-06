import stat

from nless.suggestions import (
    FilePathSuggestionProvider,
    HistorySuggestionProvider,
    ShellCommandSuggestionProvider,
    StaticSuggestionProvider,
)


class TestHistorySuggestionProvider:
    def test_empty_input_returns_history(self):
        provider = HistorySuggestionProvider(["foo", "bar"])
        assert provider.get_suggestions("") == ["bar", "foo"]

    def test_empty_history_returns_nothing(self):
        provider = HistorySuggestionProvider([])
        assert provider.get_suggestions("foo") == []

    def test_prefix_match(self):
        provider = HistorySuggestionProvider(["apple", "banana", "apricot"])
        result = provider.get_suggestions("ap")
        assert result == ["apple", "apricot"]

    def test_substring_match(self):
        provider = HistorySuggestionProvider(["hello world", "goodbye"])
        result = provider.get_suggestions("world")
        assert result == ["hello world"]

    def test_prefix_before_substring(self):
        provider = HistorySuggestionProvider(["search foo", "foo search", "foobar"])
        result = provider.get_suggestions("foo")
        assert result == ["foo search", "foobar", "search foo"]

    def test_case_insensitive(self):
        provider = HistorySuggestionProvider(["Hello", "HELLO", "hello"])
        result = provider.get_suggestions("hel")
        assert result == ["Hello", "HELLO", "hello"]

    def test_no_match(self):
        provider = HistorySuggestionProvider(["alpha", "beta"])
        assert provider.get_suggestions("xyz") == []


class TestStaticSuggestionProvider:
    def test_empty_input_shows_all(self):
        provider = StaticSuggestionProvider(["csv", "tsv", "raw"])
        assert provider.get_suggestions("") == ["csv", "tsv", "raw"]

    def test_history_before_options(self):
        provider = StaticSuggestionProvider(
            ["csv", "tsv", "raw"], history=["tsv", "raw"]
        )
        result = provider.get_suggestions("")
        # History items (not in static set) come first, then static options
        # tsv and raw are in static set so they stay in options only
        assert result == ["csv", "tsv", "raw"]

    def test_history_unique_items_prepended(self):
        provider = StaticSuggestionProvider(
            ["csv", "tsv"], history=["custom1", "custom2"]
        )
        result = provider.get_suggestions("")
        assert result[:2] == ["custom2", "custom1"]
        assert "csv" in result
        assert "tsv" in result

    def test_history_deduped_most_recent_first(self):
        provider = StaticSuggestionProvider(["csv"], history=["a", "b", "a", "c"])
        # reversed: c, a, b, a → dict.fromkeys keeps first: c, a, b
        assert provider.history == ["c", "a", "b"]

    def test_prefix_match(self):
        provider = StaticSuggestionProvider(["csv", "tsv", "raw"], history=["custom"])
        result = provider.get_suggestions("c")
        assert "csv" in result
        assert "custom" in result

    def test_substring_match(self):
        provider = StaticSuggestionProvider(["csv", "tsv", "raw"])
        result = provider.get_suggestions("sv")
        assert result == ["csv", "tsv"]

    def test_max_results(self):
        options = [f"opt{i}" for i in range(25)]
        provider = StaticSuggestionProvider(options)
        result = provider.get_suggestions("")
        assert len(result) == 20


class TestFilePathSuggestionProvider:
    def test_empty_input_lists_cwd(self):
        provider = FilePathSuggestionProvider()
        results = provider.get_suggestions("")
        # Should list current directory contents
        assert len(results) > 0

    def test_list_directory(self, tmp_path):
        (tmp_path / "file1.txt").touch()
        (tmp_path / "file2.csv").touch()
        (tmp_path / "subdir").mkdir()
        provider = FilePathSuggestionProvider()
        result = provider.get_suggestions(str(tmp_path) + "/")
        assert str(tmp_path / "file1.txt") in result
        assert str(tmp_path / "file2.csv") in result
        assert str(tmp_path / "subdir") + "/" in result

    def test_partial_filename(self, tmp_path):
        (tmp_path / "report.csv").touch()
        (tmp_path / "readme.md").touch()
        (tmp_path / "other.txt").touch()
        provider = FilePathSuggestionProvider()
        result = provider.get_suggestions(str(tmp_path / "re"))
        assert str(tmp_path / "readme.md") in result
        assert str(tmp_path / "report.csv") in result
        assert str(tmp_path / "other.txt") not in result

    def test_nonexistent_directory(self):
        provider = FilePathSuggestionProvider()
        result = provider.get_suggestions("/nonexistent_dir_xyz/")
        assert result == []

    def test_max_results(self, tmp_path):
        for i in range(25):
            (tmp_path / f"file{i:02d}.txt").touch()
        provider = FilePathSuggestionProvider()
        result = provider.get_suggestions(str(tmp_path) + "/")
        assert len(result) == 20

    def test_dirs_get_trailing_slash(self, tmp_path):
        (tmp_path / "mydir").mkdir()
        provider = FilePathSuggestionProvider()
        result = provider.get_suggestions(str(tmp_path) + "/")
        assert any(r.endswith("mydir/") for r in result)


class TestShellCommandSuggestionProvider:
    def test_empty_input(self):
        provider = ShellCommandSuggestionProvider([])
        assert provider.get_suggestions("") == []

    def test_matches_path_executables(self, tmp_path, monkeypatch):
        # Create a fake executable
        exe = tmp_path / "mytestcmd"
        exe.touch()
        exe.chmod(exe.stat().st_mode | stat.S_IXUSR)

        monkeypatch.setenv("PATH", str(tmp_path))
        provider = ShellCommandSuggestionProvider([])
        provider._scan_thread.join()
        result = provider.get_suggestions("mytest")
        assert "mytestcmd" in result

    def test_history_fallback_after_space(self):
        provider = ShellCommandSuggestionProvider(["grep foo", "grep bar", "awk stuff"])
        result = provider.get_suggestions("grep ")
        # Should use HistorySuggestionProvider with full value
        assert "grep foo" in result
        assert "grep bar" in result

    def test_max_results(self, tmp_path, monkeypatch):
        # Create many executables
        for i in range(25):
            exe = tmp_path / f"cmd{i:02d}"
            exe.touch()
            exe.chmod(exe.stat().st_mode | stat.S_IXUSR)

        monkeypatch.setenv("PATH", str(tmp_path))
        provider = ShellCommandSuggestionProvider([])
        provider._scan_thread.join()
        result = provider.get_suggestions("cmd")
        assert len(result) == 20

    def test_background_scan_populates(self):
        provider = ShellCommandSuggestionProvider([])
        provider._scan_thread.join()
        # After thread completes, executables are populated
        assert isinstance(provider._executables, list)
        assert len(provider._executables) > 0
