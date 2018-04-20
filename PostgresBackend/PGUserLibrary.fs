namespace PostgresBackend
    open System
    open System.Threading
    open MediaBrowser.Controller
    open MediaBrowser.Controller.Entities
    open MediaBrowser.Model.Serialization
    open MediaBrowser.Controller.Persistence
    open Npgsql
    module UserQueryHandlers =

        let persistUserData (conn: NpgsqlConnection) (userId: System.Guid) (key: string) (uid: UserItemData) (cancellationToken: CancellationToken) = async {
            cancellationToken.ThrowIfCancellationRequested()
            let query = """ insert into userdata (key, userId, rating,played,playCount,isFavorite,playbackPositionTicks,lastPlayedDate,AudioStreamIndex,SubtitleStreamIndex) values (@key, @userId, @rating,@played,@playCount,@isFavorite,@playbackPositionTicks,@lastPlayedDate,@AudioStreamIndex,@SubtitleStreamIndex) on conflict update """
            use cmd = new NpgsqlCommand(query, conn)
            cmd.Parameters.AddWithValue("key", key) |> ignore
            cmd.Parameters.AddWithValue("userId", userId) |> ignore
            cmd.Parameters.AddWithValue("rating", uid.Rating) |> ignore
            cmd.Parameters.AddWithValue("played", uid.Played) |> ignore
            cmd.Parameters.AddWithValue("playCount", uid.PlayCount) |> ignore      
            cmd.Parameters.AddWithValue("isFavorite", uid.IsFavorite) |> ignore
            cmd.Parameters.AddWithValue("playBackpositionticks", uid.PlaybackPositionTicks) |> ignore
            cmd.Parameters.AddWithValue("lastPlayedDate", uid.LastPlayedDate) |> ignore
            cmd.Parameters.AddWithValue("AudioStreamIndex", uid.AudioStreamIndex) |> ignore
            cmd.Parameters.AddWithValue("SubtitleStreamIndex", uid.SubtitleStreamIndex) |> ignore

            return! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Catch
            }

        let saveUserData (conn: NpgsqlConnection) (userId : System.Guid) (key) (uid: UserItemData) (cancellationToken : CancellationToken) = async {
            if isNull uid then failwith "Null user item data in save"
            if userId = System.Guid.Empty then failwith "Empty user id in save"
            if isNull key || key = "" then failwith "Null or empty key"
            let! result = persistUserData conn userId key uid cancellationToken
            return 
                match result with
                | Choice1Of2 x -> x 
                | Choice2Of2 ex -> failwith (sprintf "Error adding to db: %s" <| ex.ToString())
            }

        let persistAllUserData (conn: NpgsqlConnection) (userId: System.Guid) (userData: UserItemData []) (cancellationToken: CancellationToken) = async {
            cancellationToken.ThrowIfCancellationRequested()
            return! 
                userData
                |> Array.map
                    (fun i ->
                        saveUserData conn userId i.Key i cancellationToken
                    )
                |> Async.Parallel
                |> Async.Ignore
            }

        let saveAllUserData (conn: NpgsqlConnection) (userId: System.Guid) (userData: UserItemData []) (cancellationToken: CancellationToken) =
            if isNull userData then
                failwith "Userdata null in save user data"
            if userId = System.Guid.Empty then
                failwith "Empty user id in save all user data"
            persistAllUserData conn userId userData cancellationToken


        let getUserData conn (userId: System.Guid) key = async {
            if userId = System.Guid.Empty then failwith "User ID is empty"
            let query = """select key,userid,rating,played,playCount,isFavorite,playbackPositionTicks,lastPlayedDate,AudioStreamIndex,SubtitleStreamIndex from userdata where key = @Key and userId=@UserId"""

            use cmd =  new NpgsqlCommand(query, conn)
            cmd.Parameters.AddWithValue("Key", key) |> ignore
            cmd.Parameters.AddWithValue("UserId", userId) |> ignore
            let! res = cmd.ExecuteReaderAsync()  |> Async.AwaitTask
            let possibleUID = res |> SqlHelpers.toUserData |> Seq.tryHead
            return 
                match possibleUID with
                | None -> (failwith "User data not found")
                | Some x -> x
        }

        let getAllUserData (conn: NpgsqlConnection) (userId: System.Guid)  = async {
            if userId = System.Guid.Empty then
                failwith "Empty user id in save all user data"

            let query = """select key,userid,rating,played,playCount,isFavorite,playbackPositionTicks,lastPlayedDate,AudioStreamIndex,SubtitleStreamIndex from userdata where userId=@UserId"""

            use cmd =  new NpgsqlCommand(query, conn)
            cmd.Parameters.AddWithValue("UserId", userId) |> ignore
            let! res = cmd.ExecuteReaderAsync()  |> Async.AwaitTask
            return SqlHelpers.toUserData res
            }


        let saveUser (conn: NpgsqlConnection) (js : IJsonSerializer) (user: User) (cancellationToken: CancellationToken) = async {
            cancellationToken.ThrowIfCancellationRequested()
            if isNull user then
                failwith "User is null"
            let jsonString = js.SerializeToString user

            cancellationToken.ThrowIfCancellationRequested()
            use cmd = new NpgsqlCommand ("insert into users (guid, data) values (@Guid, @Data) on conflict update", conn)
            cmd.Parameters.AddWithValue("Guid", user.Id) |> ignore
            cmd.Parameters.AddWithValue("Data", jsonString) |> ignore
            let! result  = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask

            return ()
            }
        let deleteUser (conn: NpgsqlConnection) (user: User) (cancellationToken: CancellationToken) = async {
            cancellationToken.ThrowIfCancellationRequested()
            let query = "delete from users where guid=@guid"
            use cmd = new NpgsqlCommand(query, conn)
            cmd.Parameters.AddWithValue("guid", user.Id) |> ignore
            let! result = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
            return ()
            }


        let retrieveAllUsers (conn: NpgsqlConnection) (js : IJsonSerializer) = async {
            let query = "select guid, data from users"
            use cmd = new NpgsqlCommand(query, conn)
            let! result = cmd.ExecuteReaderAsync() |> Async.AwaitTask
            return result |> SqlHelpers.ToUser js
            }
    type PostgresUserLibrary(connectionString, js : IJsonSerializer) =
        let conn = new NpgsqlConnection(connectionString)
        let query1 = "create table if not exists users (guid GUID primary key NOT NULL, data BLOB NOT NULL)"
        let query2 = "create index if not exists idx_users on users(guid)"
        let cmd1 = new NpgsqlCommand(query1, conn)
        let cmd2 = new NpgsqlCommand(query2, conn)
        do
            conn.Open()
            [
                cmd1.ExecuteNonQueryAsync() |> Async.AwaitTask
                cmd2.ExecuteNonQueryAsync() |> Async.AwaitTask
            ]
            |> Async.Parallel
            |> Async.Ignore
            |> Async.RunSynchronously
            cmd1.Dispose()
            cmd2.Dispose()
        interface IUserRepository with
            member this.Dispose () = conn.Dispose()
            member this.Name with get() = "Postgres"
            member this.DeleteUser (user: User, cancellationToken : CancellationToken) =
                UserQueryHandlers.deleteUser conn user cancellationToken |> Async.RunSynchronously
            member this.SaveUser(user: User, cancellationToken : CancellationToken) =
                UserQueryHandlers.saveUser conn js user cancellationToken  |> Async.RunSynchronously
                ()
            member this.RetrieveAllUsers () =
                UserQueryHandlers.retrieveAllUsers conn js |> Async.RunSynchronously