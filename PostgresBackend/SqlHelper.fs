namespace PostgresBackend
    module SqlHelpers =
        open MediaBrowser.Model.Serialization
        open Npgsql
        open System.Data.Common
        open MediaBrowser.Model.Entities
        open MediaBrowser.Controller.Entities

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
                   if isNull (reader.GetValue(2)) then None
                   else Some (float (reader.GetFloat(2)))
                   |> Option.toNullable
               uid.Played <- reader.GetBoolean(3)
               uid.PlayCount <- reader.GetInt32(4)
               uid.IsFavorite <- reader.GetBoolean(5)
               uid.PlaybackPositionTicks <- reader.GetInt64(6)
               uid.LastPlayedDate <-
                   if isNull ( reader.GetValue(7)) then None
                   else Some (reader.GetDateTime(7))
                   |> Option.toNullable
               uid.AudioStreamIndex <-
                   if isNull (reader.GetValue(8)) then None
                   else Some (reader.GetInt32(8))
                   |> Option.toNullable
               uid.SubtitleStreamIndex <- 
                  if isNull (reader.GetValue(9)) then None
                  else Some (reader.GetInt32(9))
                  |> Option.toNullable


               yield uid
            }

