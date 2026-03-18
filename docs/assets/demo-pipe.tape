# VHS tape for nless pipe mode demo
# https://github.com/charmbracelet/vhs

Output docs/assets/demo-pipe.gif

Set Theme "Dracula"
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 18
Set Width 1200
Set Height 600
Set Padding 10
Set TypingSpeed 60ms

# Use nless in a pipeline — filter CSV and output as JSON
Type "cat people-1000.csv | nless -f 'Job Title=Engineer' -s 'Last Name=asc' -c 'First|Last|Job' --no-tui -o json | head -5"
Enter
Sleep 5s

# Clear terminal before interactive version
Type "clear"
Enter
Sleep 1s

# Now show the interactive version — drop --no-tui
Type "cat people-1000.csv | nless -f 'Job Title=Engineer' -s 'Last Name=asc' -c 'First|Last|Job'"
Enter
Sleep 3s

# Force a full column re-render by navigating right then back
Type "$"
Sleep 500ms
Type "0"
Sleep 2s

# Caption
Type "#"
Sleep 500ms
Type "Same transforms, but interactive — explore before piping with Q"
Sleep 3s
Escape
Sleep 2s

Sleep 2s
