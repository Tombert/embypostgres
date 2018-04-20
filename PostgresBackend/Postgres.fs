namespace PostgresBackend
open MediaBrowser.Controller
open MediaBrowser.Controller.Entities
open MediaBrowser.Controller.Persistence
open MediaBrowser.Model.Querying
open MediaBrowser.Model.IO
open MediaBrowser.Model.Logging
open MediaBrowser.Model.Entities
open MediaBrowser.Model.Serialization
open MediaBrowser.Controller.Security
open System.Data.Common
open System.Collections.Generic
open System.Threading

open Npgsql

module SqlHelpers =
    let ToDisplayPreference (js: IJsonSerializer) (reader: DbDataReader) = seq {
        while reader.Read() do
            let user = js.DeserializeFromString<DisplayPreferences>  (reader.GetString(0))
            yield user
            }

    let ToUser (js: IJsonSerializer) (reader : DbDataReader) = seq {
        //let blah = new User()
        while reader.Read() do
            let user = js.DeserializeFromString<User> (reader.GetString(1))
            let userid = reader.GetString(1)
            user.Id <- System.Guid userid
            yield user
        }


    let toUserData (reader : DbDataReader) = seq {
        while reader.Read() do
           let uid = new UserItemData()
           uid.Key <- reader.GetString(0)
           uid.UserId <- reader.GetGuid(1)
           uid.Rating <-
               if isNull (reader.GetValue(2)) then
                   None
               else
                   Some (float (reader.GetFloat(2)))
               |> Option.toNullable
           uid.Played <- reader.GetBoolean(3)
           uid.PlayCount <- reader.GetInt32(4)
           uid.IsFavorite <- reader.GetBoolean(5)
           uid.PlaybackPositionTicks <- reader.GetInt64(6)
           uid.LastPlayedDate <-
               if isNull ( reader.GetValue(7)) then
                   None
               else
                   Some (reader.GetDateTime(7))
               |> Option.toNullable
           uid.AudioStreamIndex <-
               if isNull (reader.GetValue(8)) then
                   None
               else
                   Some (reader.GetInt32(8))
               |> Option.toNullable
           uid.SubtitleStreamIndex <- 
              if isNull (reader.GetValue(9)) then
                  None
              else
                  Some (reader.GetInt32(9))
              |> Option.toNullable


           yield uid
        }

module UserQueryHandlers =
    let persistUserData (conn: NpgsqlConnection) (userId: System.Guid) (key: string) (uid: UserItemData) (cancellationToken: CancellationToken) = async {
        cancellationToken.ThrowIfCancellationRequested()
        let query = """ insert into userdata (key, userId, rating,played,playCount,isFavorite,playbackPositionTicks,lastPlayedDate,AudioStreamIndex,SubtitleStreamIndex) values (@key, @userId, @rating,@played,@playCount,@isFavorite,@playbackPositionTicks,@lastPlayedDate,@AudioStreamIndex,@SubtitleStreamIndex) on conflict update """
        let cmd = new NpgsqlCommand(query, conn)
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
        if isNull uid then
            failwith "Null user item data in save"
        if userId = System.Guid.Empty then
            failwith "Empty user id in save"
        if isNull key || key = "" then
            failwith "Null or empty key"
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
        let cmd = new NpgsqlCommand(query , conn)
        cmd.Parameters.AddWithValue("userid", userid) |> ignore
        cmd.Parameters.AddWithValue("client", client) |> ignore
        let! result = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        return SqlHelpers.ToDisplayPreference js result
        }

module AuthenticationHandlers =
    let BaseSelectText = "select Id, AccessToken, DeviceId, AppName, AppVersion, DeviceName, UserId, IsActive, DateCreated, DateRevoked from AccessTokens";
    let update (conn: NpgsqlConnection) (info: AuthenticationInfo) = async {
        let query = "insert into AccessTokens (Id, AccessToken, DeviceId, AppName, AppVersion, DeviceName, UserId, IsActive, DateCreated, DateRevoked) values (@Id, @AccessToken, @DeviceId, @AppName, @AppVersion, @DeviceName, @UserId, @IsActive, @DateCreated, @DateRevoked) on conflict update"
        let cmd = new NpgsqlCommand(query, conn)
        let dateRevoked =
            if info.DateRevoked.HasValue then
                Some (info.DateRevoked.Value)
            else None

            |> Option.toNullable
        cmd.Parameters.AddWithValue("Id", System.Guid(info.Id).ToString("N")) |> ignore
        cmd.Parameters.AddWithValue("AccessToken", info.AccessToken) |> ignore
        cmd.Parameters.AddWithValue("DeviceId", info.DeviceId) |> ignore
        cmd.Parameters.AddWithValue("AppName", info.AppName) |> ignore
        cmd.Parameters.AddWithValue("AppVersion", info.AppVersion) |> ignore
        cmd.Parameters.AddWithValue("DeviceName", info.DeviceName) |> ignore
        cmd.Parameters.AddWithValue("UserId", info.UserId) |> ignore
        cmd.Parameters.AddWithValue("IsActive", info.IsActive) |> ignore
        cmd.Parameters.AddWithValue("DateCreated", info.DateCreated) |> ignore
        cmd.Parameters.AddWithValue("DateRevoked", dateRevoked) |> ignore
        let! res = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask 
        return ()
    }
    let create conn (info : AuthenticationInfo) = async {
        let newId = System.Guid.NewGuid().ToString("N")
        info.Id <- newId
        return! update conn info 
    }

    let getWheres (query: AuthenticationInfoQuery) = seq {
        yield "1=1"
        if not (isNull query.AccessToken ) && query.AccessToken <> "" then
            yield "AccessToken=@AccessToken" 

        if not (isNull query.UserId) && query.UserId <> "" then
            yield "UserId=@UserId"

        if not (isNull query.DeviceId) && query.DeviceId <> "" then
            yield "DeviceId=@DeviceId"
        if query.IsActive.HasValue then
            yield "IsActive=@IsActive"

        if query.HasUser.HasValue then
            if query.HasUser.Value then
                yield "UserId not null"
            else
                yield "UserId is null"
         
    }



    let get conn (query: AuthenticationInfoQuery) =
        let startIndex =
            query.StartIndex
            |> Option.ofNullable
            |> (fun x -> defaultArg x 0)

        let whereTextWithoutPaging =
            query
            |> getWheres
            |> String.concat " AND "

        let finalText =
            if startIndex > 0 then ()


        ()


    let rec getQueryHelper (reader: DbDataReader) = seq {
        while reader.Read() do
            let id = reader.GetString(0)
            let myId = System.Guid(id).ToString("N")
            let accessToken = reader.GetString(1)
            let ai = AuthenticationInfo(Id = id, AccessToken = accessToken)

            let deviceId = try Some ( reader.GetString(2)) with | ex -> None
            let appName = try Some ( reader.GetString(3)) with | ex -> None
            let appVersion = try Some ( reader.GetString(4)) with | ex -> None
            let deviceName = try Some ( reader.GetString(5)) with | ex -> None
            let userId = try Some ( reader.GetString(6)) with | ex -> None
            let isActive = try Some ( reader.GetBoolean(7)) with | ex -> None
            let dateCreated = try Some ( reader.GetDateTime(8) ) with | ex -> None
            let dateRevoked = try Some ( reader.GetDateTime(9) ) with | ex -> None

            match deviceId with
            | Some x ->
                if not (isNull x) then
                    ai.DeviceId <- x
            | None -> ()

            match appName with
            | Some x ->
                if not (isNull x) then
                    ai.AppName <- x
            | None -> ()

            match appVersion with
            | Some x ->
                if not (isNull x) then
                    ai.AppVersion <- x
            | None -> ()

            match deviceName with
            | Some x ->
                if not (isNull x) then
                    ai.DeviceName <- x
            | None -> ()

            match userId with
            | Some x ->
                if not (isNull x) then
                    ai.UserId <- x
            | None -> ()

            match isActive with
            | Some x -> ai.IsActive <- x
            | None -> ()

            match dateCreated with
            | Some x -> ai.DateCreated <- x
            | None -> ()

            ai.DateRevoked <- dateRevoked |> Option.toNullable
            yield ai
    }

    let getString conn (id: string) = async {
        if isNull id || id = "" then
            failwith "Null or empty in getstring function"
        let query = sprintf "%s where Id=@Id" BaseSelectText
        let cmd = new NpgsqlCommand(query, conn)
        cmd.Parameters.AddWithValue("Id",System.Guid(id)) |> ignore
        let! res = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        let result =
            res
            |> getQueryHelper
            |> Seq.head
        return result
        }


type PostgresAuthenticationRepository (accessToken: string, id: string) =
    let connectionString = ""
    let conn = new NpgsqlConnection(connectionString)
    do conn.Open()
    interface System.IDisposable with
        member this.Dispose() =
            conn.Dispose()
    interface IAuthenticationRepository with
        member this.Get(query: AuthenticationInfoQuery) : QueryResult<AuthenticationInfo> =




            failwith ""

        member this.Update(info: AuthenticationInfo, cancellationToken: CancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()
            AuthenticationHandlers.update conn info |> Async.RunSynchronously
            ()

        member this.Get(id: string) : AuthenticationInfo =
            id
            |> AuthenticationHandlers.getString conn
            |> Async.RunSynchronously

        member this.Create (info: AuthenticationInfo, cancellationToken: CancellationToken) = 
            info
            |> AuthenticationHandlers.create conn
            |> Async.RunSynchronously
            ()

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

type PostgresUserLibrary(connectionString, js : IJsonSerializer) =
    let conn = new NpgsqlConnection(connectionString)
    do conn.Open()
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
            UserQueryHandlers.saveAllUserData conn userId userData cancellationToken |> Async.RunSynchronously
        member this.SaveUserData (userId: System.Guid, key: string, uid : UserItemData, cancellationToken : CancellationToken ) = 
            UserQueryHandlers.saveUserData conn userId key uid cancellationToken |> Async.Ignore |> Async.RunSynchronously
        member this.GetUserData((userId: System.Guid), (key: string)) =
            UserQueryHandlers.getUserData conn userId key |> Async.RunSynchronously
        member this.GetUserData(userId : System.Guid, keys : System.Collections.Generic.List<string>) =
            let possibleValue = Seq.tryHead keys
            match possibleValue with
            | None -> failwith "No data in list of keys"
            | Some x ->
                UserQueryHandlers.getUserData conn userId x |> Async.RunSynchronously


