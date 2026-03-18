# VHS tape for nless hero GIF — short hook
# https://github.com/charmbracelet/vhs

Output docs/assets/demo.gif

Set Theme "Dracula"
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 18
Set Width 1200
Set Height 600
Set Padding 10
Set TypingSpeed 60ms

# Pipe simulated k8s events into nless
Type "kubectl get events -A -w | nless"
Enter
Sleep 5s

# Caption: auto-inferred columns
Type "#"
Sleep 500ms
Type "Streaming K8s events — columns auto-inferred"
Sleep 3s
Escape
Sleep 1s

# Jump to bottom to show live data
Type "G"
Sleep 1.5s
Type "k"
Sleep 300ms
Type "k"
Sleep 300ms
Type "k"
Sleep 1.5s

# Search for warnings
Type "/"
Sleep 400ms
Type "Warning"
Sleep 400ms
Enter
Sleep 2s

# Apply search as filter
Type "&"
Sleep 2s

# Jump to REASON and sort
Type "c"
Sleep 400ms
Type "REASON"
Sleep 400ms
Enter
Sleep 800ms
Type "s"
Sleep 2s

# Filter to CrashLoopBackOff
Type "f"
Sleep 400ms
Type "CrashLoop"
Sleep 400ms
Enter
Sleep 2s

# Pivot by namespace
Type "#"
Sleep 500ms
Type "Which namespaces are crashing?"
Sleep 2.5s
Escape
Sleep 800ms

Type "0"
Sleep 400ms
Type "U"
Sleep 3s

Sleep 2s
