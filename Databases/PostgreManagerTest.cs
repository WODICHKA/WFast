//#define POSTGRE_MULTI_THREAD

using System;
using System.Collections.Generic;
using System.Text;
using Npgsql;

namespace WFast.Databases.Postgre
{
    static class PostgreHelper
    {
        public static bool IsConnected(this NpgsqlConnection _conn)
        {
            var status = _conn.State;

            if (status == System.Data.ConnectionState.Open)
                return true;
            else
            if (status == System.Data.ConnectionState.Broken || status == System.Data.ConnectionState.Closed)
                return false;
            else throw new Exception($"IsConnected() => wrong status: {status.ToString()}");
        }
    }

    public class PostgreManager
    {
        private const string ERROR_CONN_RESET = "57P01";
        private readonly static string ERROR_CONNECTION = "Exception while connecting".ToLower();

        public delegate void PostgreEvent(string Host, string DB, int retry);

        public PostgreEvent OnConnected;
        public PostgreEvent OnConnectError;
        public PostgreEvent OnDisconnected;
        private int _rcnTryDelay;
        public int ReconnectTryDelay
        {
            get { return _rcnTryDelay; }
            set
            {
                if (value <= 0)
                    _rcnTryDelay = 1000;
                else
                    _rcnTryDelay = value;
            }
        }

        private NpgsqlConnection _thisConnection;
        private NpgsqlConnectionStringBuilder connectionStringInfo;

#if POSTGRE_MULTI_THREAD
        private object _locker;
#endif

        public PostgreManager(string host, string userName, string password, string database)
        {
            connectionStringInfo = new NpgsqlConnectionStringBuilder();

            connectionStringInfo.Host = host;
            connectionStringInfo.Username = userName;
            connectionStringInfo.Password = password;
            connectionStringInfo.Database = database;

            _thisConnection = null;

            ReconnectTryDelay = 250;

#if POSTGRE_MULTI_THREAD
            _locker = new object();
#endif
        }

        private void destroyConnection(bool iternal = true)
        {
            if (_thisConnection != null)
            {
                _thisConnection.Close();
                _thisConnection = null;

                if (!iternal)
                    if (OnDisconnected != null)
                        OnDisconnected(connectionStringInfo.Host, connectionStringInfo.Database, -1);
            }
        }

        public void Close()
        {
#if POSTGRE_MULTI_THREAD
            lock (_locker)
            {
#endif
                destroyConnection(false);
#if POSTGRE_MULTI_THREAD
            }
#endif
        }

        public NpgsqlDataReader ExecuteReader(string query)
        {
#if POSTGRE_MULTI_THREAD
            lock (_locker)
            {
#endif

                NpgsqlConnection conn = getConnection();

                NpgsqlCommand command = conn.CreateCommand();

                command.CommandText = query;

                try
                {
                    return command.ExecuteReader();
                }
                catch (PostgresException postgreExp)
                {
                    if (postgreExp.Code == ERROR_CONN_RESET)
                        return ExecuteReader(query);

                    throw postgreExp;
                }
                catch (Exception e)
                {
                    throw e;
                }

#if POSTGRE_MULTI_THREAD
            }
#endif
        }
        public int ExecuteNoReturn(string query)
        {
#if POSTGRE_MULTI_THREAD
            lock (_locker)
            {
#endif
                NpgsqlConnection conn = getConnection();

                NpgsqlCommand command = conn.CreateCommand();

                command.CommandText = query;

                try
                {
                    return command.ExecuteNonQuery();
                }
                catch (PostgresException postgreExp)
                {
                    if (postgreExp.Code == ERROR_CONN_RESET)
                        return ExecuteNoReturn(query);

                    throw postgreExp;
                }
                catch (Exception e)
                {
                    throw e;
                }

#if POSTGRE_MULTI_THREAD
            }
#endif
        }

        private NpgsqlConnection getConnection(int retry = 1)
        {
            if (_thisConnection == null || !_thisConnection.IsConnected())
            {
                this.Close();

                _thisConnection = new NpgsqlConnection(connectionStringInfo.ToString());

                try
                {

                    _thisConnection.Open();

                }
                catch (NpgsqlException npgsqlConnection)
                {
                    string message = npgsqlConnection.Message.ToLower();

                    if (message != ERROR_CONNECTION)
                        throw npgsqlConnection;
                }
                catch (Exception e)
                {
                    throw e;
                }

                if (_thisConnection.IsConnected())
                {
                    if (OnConnected != null)
                        OnConnected(connectionStringInfo.Host, connectionStringInfo.Database, -1);

                    return _thisConnection;
                }
                else
                {
                    if (OnConnectError != null)
                        OnConnectError(connectionStringInfo.Host, connectionStringInfo.Database, retry);

                    destroyConnection();

                    System.Threading.Thread.Sleep(ReconnectTryDelay);

                    return getConnection(++retry);
                }
            }
            else
                return _thisConnection;
        }
    }
}
