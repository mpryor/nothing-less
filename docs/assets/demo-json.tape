# VHS tape for nless JSON demo
# https://github.com/charmbracelet/vhs

Output docs/assets/demo-json.gif

Set Theme "Dracula"
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 18
Set Width 1200
Set Height 600
Set Padding 10
Set TypingSpeed 60ms

# Pipe JSON API response into nless
Type "cat api-response.json | nless"
Enter
Sleep 3s

# Caption
Type "#"
Sleep 500ms
Type "JSON auto-detected — keys become columns"
Sleep 3s
Escape
Sleep 1s

# Filter to errors
Type "/"
Sleep 400ms
Type "error"
Sleep 400ms
Enter
Sleep 2s

Type "&"
Sleep 2s

# Sort by latency
Type "c"
Sleep 400ms
Type "latency"
Sleep 400ms
Enter
Sleep 800ms
Type "s"
Sleep 2s

# Caption
Type "#"
Sleep 500ms
Type "Slow requests causing errors — all /api/payments"
Sleep 3s
Escape
Sleep 2s

Sleep 2s
