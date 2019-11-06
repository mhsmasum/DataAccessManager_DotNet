using System.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace WebApplication1.DataManager
{
    public class DataAccessManager
    {
        private SqlConnection _sqlConnection = null;
        private SqlCommand _sqlCommand = null;
        private SqlDataReade _sqlDataReader = null;
        private SqlTransaction _sqlTransaction = null;
        private SqlDataAdapter _sqlDataAdapter = null;
        private const int _commandTimeOut = 180000; // (3 min *60000)=  milliseconds 
        private bool _isException;
        private bool returnValue = true;
        private string ConnectionString(string database)
        {
            return @"data source=" + SqlUserAccess.DataSource + ";Initial Catalog=" + database +
                   ";Integrated Security=false;MultipleActiveResultSets=true; User Id=" +
                   SqlUserAccess.UserName + "; password=" + SqlUserAccess.PassWord + ";";
        }
        //MultipleActiveResultSets=true;



    public SqlDataReader GetSqlDataReader(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            try
            {
                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };

                _sqlCommand.Parameters.Clear();
                _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
                if (IsBigData)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }



               _sqlDataReader = _sqlCommand.ExecuteReader();
                _sqlCommand.Parameters.Clear();
            }
            catch (SqlException sqlException)
            {
                _isException = true;
               _sqlDataReader = null;
                throw sqlException;
            }
            finally
            {
                _sqlCommand.Parameters.Clear();
                _sqlCommand.Dispose();
                if (_isException)
                {
                    SqlConnectionClose(true);
                }

            }
            retur _sqlDataReader;
        }
        public bool CheckIfConnectionExits(string database)
        {
            bool conne = true;
            try
            {
                _sqlConnection = new SqlConnection(ConnectionString(database));

                if (_sqlConnection.State != ConnectionState.Open)
                {
                    conne = false;

                }
            }
            catch (SqlException sqlException)
            {
                _isException = true;
                throw sqlException;
            }

            return conne;

        }

        public bool SqlConnectionOpen(string database)
        {
            try
            {
                _sqlConnection = new SqlConnection(ConnectionString(database));

                if (_sqlConnection.State != ConnectionState.Open)
                {
                    _sqlConnection.Open();
                    _sqlTransaction = _sqlConnection.BeginTransaction();

                }
            }
            catch (SqlException sqlException)
            {
                _isException = true;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    returnValue = false;
                }

            }
            return returnValue;
        }

        public bool SqlConnectionOpenAsync(string database)
        {
            try
            {
                _sqlConnection = new SqlConnection(ConnectionString(database));

                if (_sqlConnection.State != ConnectionState.Open)
                {
                    _sqlConnection.Open();
                    //_sqlTransaction = _sqlConnection.BeginTransaction();

                }
            }
            catch (SqlException sqlException)
            {
                _isException = true;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    returnValue = false;
                }

            }
            return returnValue;
        }

        public bool SqlConnectionClose(bool IsRollBack = false)
        {
            try
            {
                if (_sqlConnection.State == ConnectionState.Open)
                {
                    if (_sqlTransaction != null)
                    {
                        if (_sqlDataReader != null)
                        {
                           _sqlDataReader.Close();

                        }
                        if (IsRollBack)
                        {
                            _sqlTransaction.Rollback();
                        }
                        else
                        {
                            _sqlTransaction.Commit();
                        }
                        _sqlTransaction.Dispose();
                    }
                    _sqlCommand.Dispose();
                    _sqlConnection.Close();
                }

            }
            catch (SqlException sqlException)
            {
                _isException = true;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    returnValue = false;
                }

            }

            return returnValue;
        }

        public bool SqlConnectionCloseAsync()
        {
            try
            {
                if (_sqlConnection.State == ConnectionState.Open)
                {
                    _sqlCommand.Dispose();
                    _sqlConnection.Close();
                }

            }
            catch (SqlException sqlException)
            {
                _isException = true;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    returnValue = false;
                }

            }

            return returnValue;
        }
        public SqlDataReader GetSqlDataReader(string StoreProcedure, bool IsBigData = false)
        {
            try
            {
                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };
                if (IsBigData)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }

               _sqlDataReader = _sqlCommand.ExecuteReader();
            }
            catch (SqlException sqlException)
            {
                _isException = true;
               _sqlDataReader = null;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    SqlConnectionClose(true);
                }

            }
            retur _sqlDataReader;
        }
       
        public DataTable GetDataTable(string StoreProcedure, bool IsBigData = false)
        {
            DataTable dt = new DataTable();

            try
            {
                DataSet ds = new DataSet();
                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };

                if (IsBigData)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                sqlDataAdapter = new SqlDataAdapter(_sqlCommand);
                sqlDataAdapter.Fill(ds);
                dt = ds.Tables[0];
            }
            catch (SqlException sqlException)
            {
                _isException = true;
                dt = null;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    SqlConnectionClose(true);
                }

            }
            return dt;
        }
        public DataTable GetDataTable(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            DataTable dt = new DataTable();

            try
            {
                DataSet ds = new DataSet();
                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };

                _sqlCommand.Parameters.Clear();
                _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
                if (IsBigData==true)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                sqlDataAdapter = new SqlDataAdapter(_sqlCommand);
                sqlDataAdapter.Fill(ds);
                dt = ds.Tables[0];

            }
            catch (SqlException sqlException)
            {
                _isException = true;
                dt = null;
                throw sqlException;
            }
            finally
            {
                _sqlCommand.Parameters.Clear();
                if (_isException)
                {
                    SqlConnectionClose(true);
                }
            }
            return dt;
        }

        //public async Task<DataTable> GetDataTableAsync(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        //{
        //    DataTable dt = new DataTable();

        //    try
        //    {
        //        DataSet ds = new DataSet();
        //        _sqlCommand = new SqlCommand
        //        {
        //            Connection = _sqlConnection,
        //            CommandType = CommandType.StoredProcedure,
        //            CommandText = StoreProcedure
        //            //,
        //            //Transaction = _sqlTransaction
        //        };

        //        _sqlCommand.Parameters.Clear();
        //        _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
        //        if (IsBigData)
        //        {
        //            _sqlCommand._commandTimeOut = _commandTimeOut;
        //        }
        //        sqlDataAdapter = new SqlDataAdapter(_sqlCommand);
        //        //sqlDataAdapter.Fill(ds);
        //        await Task.Run(() => sqlDataAdapter.Fill(ds));
        //        dt = ds.Tables[0];

        //    }
        //    catch (SqlException sqlException)
        //    {
        //        _isException = true;
        //        dt = null;
        //        throw sqlException;
        //    }
        //    finally
        //    {
        //        _sqlCommand.Parameters.Clear();
        //        if (_isException)
        //        {
        //            SqlConnectionClose(true);
        //        }
        //    }
        //    return dt;
        //}


        public DataTable GetDataTableAsync(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            DataTable dt = new DataTable();

            try
            {
                DataSet ds = new DataSet();
                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure
                    //,
                    //Transaction = _sqlTransaction
                };

                _sqlCommand.Parameters.Clear();
                _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
                if (IsBigData ==true)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                sqlDataAdapter = new SqlDataAdapter(_sqlCommand);
                sqlDataAdapter.Fill(ds);

                dt = ds.Tables[0];

            }
            catch (SqlException sqlException)
            {
                _isException = true;
                dt = null;
                throw sqlException;
            }
            finally
            {
                _sqlCommand.Parameters.Clear();
                if (_isException)
                {
                    SqlConnectionCloseAsync();
                }
            }
            return dt;
        }
        public DataSet GetDataSet(string StoreProcedure, bool IsBigData = false)
        {
            DataSet ds = new DataSet();
            try
            {

                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };

                if (IsBigData)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                sqlDataAdapter = new SqlDataAdapter(_sqlCommand);
                sqlDataAdapter.Fill(ds);
            }
            catch (SqlException sqlException)
            {
                _isException = true;
                ds = null;
                throw sqlException;
            }
            finally
            {
                if (_isException)
                {
                    SqlConnectionClose(true);
                }

            }
            return ds;
        }
        public DataSet GetDataSet(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            DataSet ds = new DataSet();
            try
            {

                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };

                _sqlCommand.Parameters.Clear();
                _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
                if (IsBigData==true)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                sqlDataAdapter = new SqlDataAdapter(_sqlCommand);
                sqlDataAdapter.Fill(ds);

            }
            catch (SqlException sqlException)
            {
                _isException = true;
                ds = null;
                throw sqlException;
            }
            finally
            {
                _sqlCommand.Parameters.Clear();
                if (_isException)
                {
                    SqlConnectionClose(true);
                }

            }
            return ds;
        }
        private int ExecuteNonQueryData(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsPrimaryKey = true, bool IsBigData = false)
        {
            int primaryKey = 0;
            try
            {
                _sqlCommand = new SqlCommand();

                _sqlCommand.Connection = _sqlConnection;
                _sqlCommand.CommandType = CommandType.StoredProcedure;
                _sqlCommand.CommandText = StoreProcedure;
                _sqlCommand.Transaction = _sqlTransaction;

                _sqlCommand.Parameters.Clear();
                _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
                if (IsBigData)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                primaryKey = Convert.ToInt32(_sqlCommand.ExecuteScalar());

            }
            catch (SqlException sqlException)
            {
                returnValue = false;
                _isException = true;
                throw sqlException;
            }
            finally
            {
                _sqlCommand.Parameters.Clear();
                if (_isException)
                {
                    SqlConnectionClose(true);
                }
            }
            return primaryKey;
        }
        /// <summary>
        /// Save Data And Return Primary Key Of Inserted Data
        /// </summary>
        /// <param name="StoreProcedure"></param>
        /// <param name="SqlParameterList"></param>
        /// <returns></returns>
        public int SaveDataReturnPrimaryKey(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            int primaryKey = 0;
            try
            {
                primaryKey = ExecuteNonQueryData(StoreProcedure, SqlParameterList, true, IsBigData);
            }
            catch (SqlException sqlException)
            {
                throw sqlException;
            }
            return primaryKey;
        }
        private bool ExecuteNonQueryData(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            try
            {
                _sqlCommand = new SqlCommand
                {
                    Connection = _sqlConnection,
                    CommandType = CommandType.StoredProcedure,
                    CommandText = StoreProcedure,
                    Transaction = _sqlTransaction
                };
                _sqlCommand.Parameters.Clear();
                _sqlCommand.Parameters.AddRange(SqlParameterList.ToArray());
                if (IsBigData == true)
                {
                    _sqlCommand._commandTimeOut = _commandTimeOut;
                }
                _sqlCommand.ExecuteNonQuery();
            }
            catch (SqlException sqlException)
            {
                returnValue = false;
                _isException = true;
                throw sqlException;
            }
            finally
            {
                _sqlCommand.Parameters.Clear();
                if (_isException)
                {
                    SqlConnectionClose(true);
                }
            }

            return returnValue;
        }
        /// <summary>
        /// After Saving Data a Boolean will be Returned
        /// </summary>
        /// <param name="StoreProcedure"></param>
        /// <param name="SqlParameterList"></param>
        /// <returns></returns>
        public bool SaveData(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            try
            {
                returnValue = ExecuteNonQueryData(StoreProcedure, SqlParameterList, IsBigData);
            }
            catch (SqlException sqlException)
            {
                returnValue = false;
                throw sqlException;
            }
            return returnValue;
        }
        /// <summary>
        /// After Updating Data a Boolean will be Returned
        /// </summary>
        /// <param name="StoreProcedure"></param>
        /// <param name="SqlParameterList"></param>
        /// <returns></returns>
        public bool UpdateData(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            try
            {
                returnValue = ExecuteNonQueryData(StoreProcedure, SqlParameterList, IsBigData);
            }
            catch (SqlException sqlException)
            {
                returnValue = false;
                throw sqlException;
            }
            return returnValue;
        }
        /// <summary>
        /// After Deleting Data a Boolean will be Returned
        /// </summary>
        /// <param name="StoreProcedure"></param>
        /// <param name="SqlParameterList"></param>
        /// <returns></returns>
        public bool DeleteData(string StoreProcedure, List<SqlParameter> SqlParameterList, bool IsBigData = false)
        {
            try
            {
                returnValue = ExecuteNonQueryData(StoreProcedure, SqlParameterList, IsBigData);
            }
            catch (SqlException sqlException)
            {
                returnValue = false;
                throw sqlException;
            }
            return returnValue;
        }
    }
}
