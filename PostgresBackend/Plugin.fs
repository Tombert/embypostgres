namespace PostgresBackend.Metadata
open System
open MediaBrowser.Model.Serialization
open PostgresBackend.Configuration

type Plugin(applicationPaths,  xmlSerializer) = 
    inherit MediaBrowser.Common.Plugins.BasePlugin<PluginConfiguration>(applicationPaths, xmlSerializer)
    let plugname = "Postgres"
    let id = Guid("48c0c41e-ae27-4133-b890-10c235f00fcf")
    let description = """A backend to use Postgrs"""

    override this.Name
        with get() = plugname 

