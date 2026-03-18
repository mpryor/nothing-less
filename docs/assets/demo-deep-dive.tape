# VHS tape for nless deep-dive GIF — drill into pod logs
# https://github.com/charmbracelet/vhs

Output docs/assets/demo-deep-dive.gif

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

# Quickly filter to CrashLoopBackOff warnings and pivot
Type "/"
Sleep 300ms
Type "Warning"
Sleep 300ms
Enter
Sleep 1s
Type "&"
Sleep 1.5s

Type "c"
Sleep 300ms
Type "REASON"
Sleep 300ms
Enter
Sleep 500ms
Type "f"
Sleep 300ms
Type "CrashLoop"
Sleep 300ms
Enter
Sleep 1.5s

Type "0"
Sleep 300ms
Type "U"
Sleep 3s

# Drill into payments namespace
Type "#"
Sleep 500ms
Type "payments has the most crashes — drill in"
Sleep 3s
Escape
Sleep 1s

Enter
Sleep 3s

# Jump to OBJECT column and copy pod name
Type "#"
Sleep 500ms
Type "Jump to OBJECT column and copy the pod name"
Sleep 3s
Escape
Sleep 1s

Type "c"
Sleep 500ms
Type "OBJECT"
Sleep 500ms
Enter
Sleep 1s
Type "y"
Sleep 2s

# Run kubectl logs via shell command
Type "#"
Sleep 500ms
Type "Check the pod's logs"
Sleep 3s
Escape
Sleep 1s

Type "!"
Sleep 500ms
Type "kubectl logs -f pod/payment-processor-7a3f1-b92e4"
Sleep 500ms
Enter
Sleep 6s

# Spring Boot format auto-detected
Type "#"
Sleep 500ms
Type "Spring Boot log format auto-detected"
Sleep 3s
Escape
Sleep 1s

# Search and filter to ERROR
Type "/"
Sleep 500ms
Type "ERROR"
Sleep 500ms
Enter
Sleep 2s

Type "#"
Sleep 500ms
Type "Filter to ERROR — what's causing the crashes?"
Sleep 3s
Escape
Sleep 1s

Type "&"
Sleep 3s

# View unparsed lines (stack traces)
Type "#"
Sleep 500ms
Type "Stack traces didn't match the log format — check with ~"
Sleep 3s
Escape
Sleep 1s

Type "~"
Sleep 4s

Type "#"
Sleep 500ms
Type "HikariPool connection timeout — root cause found"
Sleep 4s
Escape
Sleep 2s

# Close unparsed view
Type "q"
Sleep 2s

# Switch theme
Type "#"
Sleep 500ms
Type "10 built-in themes"
Sleep 2.5s
Escape
Sleep 1s

Type "T"
Sleep 500ms
Type "dracula"
Sleep 500ms
Enter
Sleep 3s

Sleep 3s
