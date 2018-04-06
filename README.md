Postgres Emby
=============

Emby is great, but there is one *severe* limitation with it: the direct use of SQLite, making it impossible to spread across multiple nodes, completely eliminating the possibility of horizontal scaling. 

My attempt to fix this is to extend the already-existent database interfaces by the emby team and write a driver to talk to PostgreSQL.  

I decided to write this in F# for presumably masochistic reasons. 

## More Information ##

[How to Build a Server Plugin](https://github.com/MediaBrowser/MediaBrowser/wiki/How-to-build-a-Server-Plugin)
