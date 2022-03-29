function Terminate-Process() {
    [CmdletBinding()]
	param(
		[Parameter()]
		[string] $ProcessID
	)

    $encodedCommand = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes("Add-Type -Names 'w' -Name 'k' -M '[DllImport(""kernel32.dll"")]public static extern bool FreeConsole();[DllImport(""kernel32.dll"")]public static extern bool AttachConsole(uint p);[DllImport(""kernel32.dll"")]public static extern bool SetConsoleCtrlHandler(uint h, bool a);[DllImport(""kernel32.dll"")]public static extern bool GenerateConsoleCtrlEvent(uint e, uint p);public static void SendCtrlC(uint p){FreeConsole();AttachConsole(p);GenerateConsoleCtrlEvent(0, 0);}';[w.k]::SendCtrlC($ProcessID)"))
    start-process powershell.exe -argument "-nologo -noprofile -executionpolicy bypass -EncodedCommand $encodedCommand"
}

$ProcessIDs=(get-process skyd).id
$Proc1=($ProcessIDs -split '\n')[0]
$Proc2=($ProcessIDs -split '\n')[1]
$Proc3=($ProcessIDs -split '\n')[2]

Terminate-Process -ProcessID $Proc1
Terminate-Process -ProcessID $Proc2
Terminate-Process -ProcessID $Proc3
