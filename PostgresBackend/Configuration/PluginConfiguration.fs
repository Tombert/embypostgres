namespace PostgresBackend.Configuration
open MediaBrowser.Model.Plugins

type PluginConfiguration () = 
    inherit BasePluginConfiguration()
    member val EnableExtractionDuringLibraryScan = true with get, set
    member val EnableLocalMediaFolderSaving = true with get, set