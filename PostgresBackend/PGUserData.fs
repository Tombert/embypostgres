namespace PostgresBackend
open MediaBrowser.Controller
open MediaBrowser.Controller.Entities
open MediaBrowser.Model.Serialization
open MediaBrowser.Controller.Persistence
open Npgsql
open System.Threading
type PostgresUserDataLibrary(connectionString) =
    let conn = new NpgsqlConnection(connectionString)
    do conn.Open()
    interface IUserDataRepository with
        member this.Name with get() = "Postgres"
        member this.GetAllUserData(userId:System.Guid) =
            userId
            |> UserQueryHandlers.getAllUserData conn
            |> Async.RunSynchronously
            |> System.Collections.Generic.List

        member this.Dispose() =
            conn.Dispose()

        member this.SaveAllUserData (userId: System.Guid, userData : UserItemData [], cancellationToken : CancellationToken) =
            PostgresBackend.UserQueryHandlers.saveAllUserData conn userId userData cancellationToken |> Async.RunSynchronously
        member this.SaveUserData (userId: System.Guid, key: string, uid : UserItemData, cancellationToken : CancellationToken ) = 
            UserQueryHandlers.saveUserData conn userId key uid cancellationToken |> Async.Ignore |> Async.RunSynchronously
        member this.GetUserData((userId: System.Guid), (key: string)) =
            UserQueryHandlers.getUserData conn userId key |> Async.RunSynchronously
        member this.GetUserData(userId : System.Guid, keys : System.Collections.Generic.List<string>) =
            let possibleValue = Seq.tryHead keys
            match possibleValue with
            | None -> failwith "No data in list of keys"
            | Some x -> UserQueryHandlers.getUserData conn userId x |> Async.RunSynchronously


