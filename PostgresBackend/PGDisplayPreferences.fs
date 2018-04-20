namespace PostgresBackend

open MediaBrowser.Model.Serialization

open MediaBrowser.Controller
open MediaBrowser.Controller.Entities
open MediaBrowser.Controller.Persistence
open Npgsql

open System.Threading
open MediaBrowser.Model.Logging
open MediaBrowser.Model.Entities
open MediaBrowser.Model.Serialization
open MediaBrowser.Controller.Security
module DisplayPreferenceHandlers = 
    let getAllDisplayPreferences conn js (userId: System.Guid) = async {
        let query = """select id, userid, client, data from userdisplaypreferences where userid = @userid"""
        use cmd = new NpgsqlCommand(query, conn)
        cmd.Parameters.AddWithValue("userid", userId) |> ignore
        let! result = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        return SqlHelpers.ToDisplayPreference js result
    }

    let saveDisplayPreferences (conn: NpgsqlConnection) (js: IJsonSerializer) (displayPreferences : DisplayPreferences) (userId: System.Guid) (client: string) (cancellationToken: CancellationToken) = async {
        cancellationToken.ThrowIfCancellationRequested()
        let query = """insert into userdisplaypreferences (id, userid, client, data) values (@id, @userid, @client, @data) on conflict update; """
        let datastring = js.SerializeToString displayPreferences
        cancellationToken.ThrowIfCancellationRequested()
        use cmd = new NpgsqlCommand(query, conn)
        cmd.Parameters.AddWithValue("userid", userId) |> ignore
        cmd.Parameters.AddWithValue("id", displayPreferences.Id) |> ignore
        cmd.Parameters.AddWithValue("client", client) |> ignore
        cmd.Parameters.AddWithValue("data", datastring) |> ignore
        let! res = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
        return ()
        }

    let saveAllDisplayPreferences conn js (displayPreferences: #seq<DisplayPreferences> ) (userid: System.Guid) (cancellationToken: CancellationToken) = 
        cancellationToken.ThrowIfCancellationRequested()
        displayPreferences
        |> Seq.map (fun x ->
            saveDisplayPreferences conn js x userid x.Client cancellationToken
        )
        |> Async.Parallel
        |> Async.Ignore

    let getDisplayPreferences conn js (dpid : string) (userid: string ) (client: string) =  async {
        let query = """select data from userdisplaypreferences where userid = @userid and client = @client"""
        use cmd = new NpgsqlCommand(query , conn)
        cmd.Parameters.AddWithValue("userid", userid) |> ignore
        cmd.Parameters.AddWithValue("client", client) |> ignore
        let! result = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        return SqlHelpers.ToDisplayPreference js result
        }
type PostgresDisplayPreferencesRepository(connectionString, js : IJsonSerializer) =
    let conn = new NpgsqlConnection(connectionString)
    do conn.Open()
    interface IDisplayPreferencesRepository with
        member this.Name with get () = "Postgres"
        member this.Dispose() = conn.Dispose()
        member this.SaveDisplayPreferences (displayPreferences, userid, client, cancellationToken) =
            DisplayPreferenceHandlers.saveDisplayPreferences conn js displayPreferences (System.Guid(userid)) client cancellationToken |> Async.RunSynchronously

        member this.GetAllDisplayPreferences (userId) =
            DisplayPreferenceHandlers.getAllDisplayPreferences conn js userId |> Async.RunSynchronously

            //failwith "placeholder for getalldisplaypreferences"
        member this.SaveAllDisplayPreferences (displayPreferences, userid, cancellationToken) = //failwith "placeholder for savealldisplaypreferences"
            DisplayPreferenceHandlers.saveAllDisplayPreferences conn js displayPreferences userid cancellationToken |> Async.RunSynchronously
        member this.GetDisplayPreferences (displayPreferenceId, userid, client) = failwith "placeholder for getdisplaypreferences"
