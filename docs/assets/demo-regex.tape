# VHS tape for nless regex delimiter demo
# https://github.com/charmbracelet/vhs

Output docs/assets/demo-regex.gif

Set Theme "Dracula"
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 18
Set Width 1200
Set Height 600
Set Padding 10
Set TypingSpeed 60ms

# Open raw log file
Type "nless app.log"
Enter
Sleep 3s

# Caption — raw text, no columns
Type "#"
Sleep 500ms
Type "Raw log file — no structure yet"
Sleep 3s
Escape
Sleep 1s

# Apply regex delimiter with named capture groups
Type "D"
Sleep 500ms
Type "(?P<host>[^~]+)~(?P<time>[^~]+)~(?P<level>[^~]+)~(?P<message>.*)"
Sleep 500ms
Enter
Sleep 2s

# Dismiss "save as log format?" prompt
Escape
Sleep 1s

# Caption
Type "#"
Sleep 500ms
Type "Regex with named groups — instant columns"
Sleep 3s
Escape
Sleep 1s

# Filter to errors
Type "c"
Sleep 400ms
Type "level"
Sleep 400ms
Enter
Sleep 800ms
Type "f"
Sleep 400ms
Type "ERROR"
Sleep 400ms
Enter
Sleep 2s

# Caption
Type "#"
Sleep 500ms
Type "Filtered to ERROR — connection pool is the culprit"
Sleep 3s
Escape
Sleep 2s

Sleep 2s
