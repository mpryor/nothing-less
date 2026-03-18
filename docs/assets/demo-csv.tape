# VHS tape for nless CSV demo GIF
# https://github.com/charmbracelet/vhs

Output docs/assets/demo-csv.gif

Set Theme "Dracula"
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 18
Set Width 1200
Set Height 600
Set Padding 10
Set TypingSpeed 60ms

# Open a CSV file directly
Type "nless example_data/people-1000.csv"
Enter
Sleep 3s

# Caption: auto-detected CSV
Type "#"
Sleep 500ms
Type "1000-row CSV — delimiter auto-detected"
Sleep 3s
Escape
Sleep 1s

# Search for a job title
Type "/"
Sleep 400ms
Type "Engineer"
Sleep 400ms
Enter
Sleep 2s

# Apply as filter
Type "&"
Sleep 2s

# Sort by Last Name
Type "c"
Sleep 400ms
Type "Last"
Sleep 400ms
Enter
Sleep 800ms
Type "s"
Sleep 2s

# Pivot by Sex
Type "c"
Sleep 400ms
Type "Sex"
Sleep 400ms
Enter
Sleep 800ms
Type "U"
Sleep 3s

# Switch theme
Type "T"
Sleep 500ms
Type "nord"
Sleep 500ms
Enter
Sleep 3s

Sleep 2s
