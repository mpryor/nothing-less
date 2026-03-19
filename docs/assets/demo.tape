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

# Unique key on NAMESPACE — watch counts update in real time
Type "0"
Sleep 400ms
Type "U"
Sleep 8s

# Back to raw data
Type "H"
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
Sleep 1s
Type "REASON"
Sleep 1s
Enter
Sleep 2s
Type "s"
Sleep 2s

# Filter to CrashLoopBackOff
Type "f"
Sleep 1s
Type "CrashLoop"
Sleep 400ms
Enter
Sleep 2s

Sleep 2s
