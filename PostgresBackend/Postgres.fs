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
open System.Globalization
open System.Collections.Generic
open System.Threading

open Npgsql



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
            | Some x -> if not (isNull x) then ai.DeviceId <- x
            | None -> ()

            match appName with
            | Some x -> if not (isNull x) then ai.AppName <- x
            | None -> ()

            match appVersion with
            | Some x -> if not (isNull x) then ai.AppVersion <- x
            | None -> ()

            match deviceName with
            | Some x -> if not (isNull x) then ai.DeviceName <- x
            | None -> ()

            match userId with
            | Some x -> if not (isNull x) then ai.UserId <- x
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


    let get conn (query: AuthenticationInfoQuery) = async {
        let res = new QueryResult<AuthenticationInfo>()
        let startIndex =
            query.StartIndex
            |> Option.ofNullable
            |> (fun x -> defaultArg x 0)

        let whereTextWithoutPaging =
            query
            |> getWheres
            |> String.concat " AND "

        let pageText =
            if startIndex > 0 then 
                let w = sprintf "Id NOT IN (SELECT Id FROM AccessTokens %s ORDER BY DateCreated LIMIT %i)" whereTextWithoutPaging startIndex
                sprintf "%s AND %s"  whereTextWithoutPaging w
            else
                whereTextWithoutPaging
        let pageTextWithOrder = sprintf "%s ORDER BY DateCreated" pageText //  [| pageText; "ORDER BY"; "DateCreated" |] |> String.concat " "
        let finalClauses =
            if query.Limit.HasValue then
                sprintf "%s LIMIT %s" pageTextWithOrder (query.Limit.Value.ToString(new CultureInfo("en-US")))
            else
                pageTextWithOrder

        let finalQuery = sprintf "%s %s" BaseSelectText finalClauses
        use countCommand = new NpgsqlCommand ((sprintf "select count (Id) from AccessTokens %s" whereTextWithoutPaging), conn)
        use! result = countCommand.ExecuteReaderAsync() |> Async.AwaitTask 
        do! result.ReadAsync() |> Async.AwaitTask |> Async.Ignore
        let count = result.GetInt32(0)
        use cmd = new NpgsqlCommand(finalQuery, conn)
        use! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
        let yo = getQueryHelper reader
        res.Items <- yo |> Seq.toArray




        return res
        }



    let getString conn (id: string) = async {
        if isNull id || id = "" then failwith "Null or empty in getstring function"
        let query = sprintf "%s where Id=@Id" BaseSelectText
        use cmd = new NpgsqlCommand(query, conn)
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
    let query1 = "create table if not exists ActivityLogEntries (Id GUID PRIMARY KEY NOT NULL, Name TEXT NOT NULL, Overview TEXT, ShortOverview TEXT, Type TEXT NOT NULL, ItemId TEXT, UserId TEXT, DateCreated DATETIME NOT NULL, LogSeverity TEXT NOT NULL)"
    let query2 = "create index if not exists idx_ActivityLogEntries on ActivityLogEntries(Id)"
    let cmd = new NpgsqlCommand(query1, conn)
    let cmd2 = new NpgsqlCommand(query2, conn)
    do
        conn.Open()
        [cmd.ExecuteNonQueryAsync() |> Async.AwaitTask; cmd2.ExecuteNonQueryAsync() |> Async.AwaitTask]
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously


    interface System.IDisposable with
        member this.Dispose() =
            conn.Dispose()
    interface IAuthenticationRepository with
        member this.Get(query: AuthenticationInfoQuery) : QueryResult<AuthenticationInfo> =
            query
            |> AuthenticationHandlers.get conn
            |> Async.RunSynchronously

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


