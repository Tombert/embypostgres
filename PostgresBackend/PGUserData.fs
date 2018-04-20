namespace PostgresBackend
open MediaBrowser.Controller
open MediaBrowser.Controller.Entities
open MediaBrowser.Model.Serialization
open MediaBrowser.Controller.Persistence
open Npgsql
open System.Threading
type PostgresUserDataLibrary(connectionString) =
    let conn = new NpgsqlConnection(connectionString)
    let queries = 
        [|
            "create table if not exists userdata (key nvarchar not null, userId GUID not null, rating float null, played bit not null, playCount int not null, isFavorite bit not null, playbackPositionTicks bigint not null, lastPlayedDate datetime null)"

            "create table if not exists DataSettings (IsUserDataImported bit)"

            "drop index if exists idx_userdata"
            "drop index if exists idx_userdata1"
            "drop index if exists idx_userdata2"
            "drop index if exists userdataindex1"

            "create unique index if not exists userdataindex on userdata (key, userId)"
            "create index if not exists userdataindex2 on userdata (key, userId, played)"
            "create index if not exists userdataindex3 on userdata (key, userId, playbackPositionTicks)"
            "create index if not exists userdataindex4 on userdata (key, userId, isFavorite)"

         |]
    do conn.Open()
       queries
       |> Array.map
           (fun x -> async {
               use cmd = new NpgsqlCommand(x, conn)
               return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
            })
       |> Async.Parallel
       |> Async.Ignore
       |> Async.RunSynchronously
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


