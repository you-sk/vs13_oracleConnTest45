using System;
using System.Text;
using System.Data.Common;
using System.Data;

namespace oracleTsushin_test001
{
    class OracleSelectTest
    {
        //デリゲートを定義
        private delegate DbProviderFactory DlgFactory();
        private delegate string DlgConn(DbProviderFactory fct);
        private delegate void DlgAccessMethod(DbProviderFactory fct, DbConnection cnn, string SQL);

        //[Main]メイン処理
        static void Main(string[] args)
        {
            //デリゲートにメソッドをセット
            DlgConn cnn = ConnectionStringORA; //接続先情報(共通)
            //接続プロバイダ
            DlgFactory odpfct = FactoryODP; //ODP.Net(Oracle実装)
            DlgFactory adofct = FactoryADO; //ADO.Net(MS実装：廃止される)
            //実装方法
            DlgAccessMethod exrd = UseExecuteReader; //executeReadeによる実装
            DlgAccessMethod dtap = UseDataAdapter; //dataAdapterによる実装

            //データ取込み
            Console.WriteLine("[1.ODP.Net(Oracle実装) & executeReader]");
            DbAccess(odpfct, cnn, exrd);

            Console.WriteLine("[2.ADO.Net(MS実装) & executeReader]");
            DbAccess(adofct, cnn, exrd);

            Console.WriteLine("[3.ODP.Net(Oracle実装) & dataAdapter]");
            DbAccess(odpfct, cnn, dtap);

            Console.WriteLine("[4.ADO.Net(MS実装) & dataAdapter]");
            DbAccess(adofct, cnn, dtap);
        }
        //[DbAccess]executeReaderによるデータ出力の実装
        private static void DbAccess(
            DlgFactory targetFct, DlgConn targetCnn, DlgAccessMethod targetAccessMethod)
        {
            //接続プロバイダの指定(ファクトリオブジェクト)
            DbProviderFactory fct = targetFct();
            //接続の作成
            using (DbConnection cnn = fct.CreateConnection())
            {
                //接続文字列のセット
                cnn.ConnectionString = targetCnn(fct);
                //データ抽出
                targetAccessMethod(fct, cnn, GetSQLString());
            }
        }
        //[UseExecuteReader]executeReaderによる実装
        private static void UseExecuteReader(
            DbProviderFactory fct, DbConnection cnn, string sql)
        {
            //SQLコマンドのセット
            DbCommand cmd = setCmd(fct, cnn, GetSQLString());
            //出力用stringBuilder
            StringBuilder sb = new StringBuilder();

            //接続の開始
            cnn.Open();

            //executeReaderによるデータ抽出
            DbDataReader reader = cmd.ExecuteReader();

            //データベース接続状態
            Console.WriteLine("CONNECT:" + cnn.State);

            //項目名の取得
            sb.Length = 0;
            //フィールド数でループしつつVALUE値を取得・結合
            for (int i = 0; i < reader.FieldCount; i++)
            { sb.Append(reader.GetName(i).ToString()).Append(","); }
            //データ出力
            sb.Length -= 1; //末尾のカンマを消す
            Console.WriteLine(sb.ToString());

            //データの取得
            while (reader.Read())
            {
                sb.Length = 0;
                //フィールド数でループしつつVALUE値を取得・結合
                for (int i = 0; i < reader.FieldCount; i++)
                { sb.Append(reader.GetValue(i).ToString()).Append(","); }
                //データ出力
                sb.Length -= 1; //末尾のカンマを消す
                Console.WriteLine(sb.ToString());
            }
            //接続の終了
            cnn.Close();
        }
        //[UseDataAdapter]DataAdapterによる実装
        private static void UseDataAdapter(
            DbProviderFactory fct, DbConnection cnn, string sql)
        {
            //SQLコマンドのセット
            DbCommand cmd = setCmd(fct, cnn, GetSQLString());
            //出力用stringBuilder
            StringBuilder sb = new StringBuilder();
            //dataAdapterを用意してcmdをセット
            DbDataAdapter da = fct.CreateDataAdapter();
            da.SelectCommand = cmd;

            

            //格納用のdatatableを用意
            DataTable dt = new DataTable();
            //fillメソッドでdataTableに格納
            da.Fill(dt);
            //データベース接続状態
            Console.WriteLine("CONNECT:" + cnn.State);

            //項目名の取得
            sb.Length = 0;
            //フィールド数でループしつつVALUE値を取得・結合
            for (int i = 0; i < dt.Columns.Count; i++)
            { sb.Append(dt.Columns[i].ColumnName).Append(","); }
            //データ出力
            sb.Length -= 1; //末尾のカンマを消す
            Console.WriteLine(sb.ToString());

            //データの取得
            foreach (DataRow dr in dt.Rows)
            {
                sb.Length = 0;
                //フィールド数でループしつつcolumn値を取得・結合
                for (int i = 0; i < dt.Columns.Count; i++)
                { sb.Append(dr[i].ToString()).Append(","); }
                //データ出力
                sb.Length -= 1; //末尾のカンマを消す
                Console.WriteLine(sb.ToString());
            }
        }
        //[GetSQLString]SQL文を構築
        private static string GetSQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT 'ORG' ID,'オレンジ' NAME,100 PLICE FROM DUAL");
            sb.Append(" UNION ");
            sb.Append("SELECT 'BNN' ID,'バナナ' NAME,80  PLICE FROM DUAL");
            return sb.ToString();
            //11gr2のodp4をインストールするとNLS_LANGに「Japanese_Japan.JA16SJISTILDE」をセットしておかないとMS実装側で文字化け
        }
        //[setCmd]DbCommandを設定する
        private static DbCommand setCmd(
            DbProviderFactory fct, DbConnection cnn, string sqlstring)
        {
            DbCommand cmd = fct.CreateCommand();
            cmd.Connection = cnn;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sqlstring;
            return cmd;
        }
        //[FactoryODP]ODP接続用DbProviderFactory
        private static DbProviderFactory FactoryODP()
        { return DbProviderFactories.GetFactory("Oracle.DataAccess.Client"); }

        //[FactoryADO]ADO接続用DbProviderFactory
        private static DbProviderFactory FactoryADO()
        { return DbProviderFactories.GetFactory("System.Data.OracleClient"); }

        //[ConnectionStringORA]Oracle接続用ConnectionString(ODP/ADO共通)
        private static string ConnectionStringORA(DbProviderFactory factory)
        {
            DbConnectionStringBuilder csb = factory.CreateConnectionStringBuilder();
            csb.Add("Data Source", "xxx.xxx.xxx.xxx:9999/SID");
            csb.Add("User ID", "USERID");
            csb.Add("Password", "PASSWORD");
            
            return csb.ConnectionString;
        }
    }
}
