package com.github.yafeiwang1240.sparkoperator.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

public class HiveMetaStore {

    static {
        System.setProperty("java.security.krb5.realm", "TIGER.COM");
        System.setProperty("java.security.krb5.kdc", "10.110.14.235");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    }

    public static void main(String[] args) throws TException, IOException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource("core-site.xml");
        hiveConf.addResource("hive-site.xml");
        UserGroupInformation.setConfiguration(hiveConf);
        UserGroupInformation.loginUserFromKeytab("hive/dig-hadoop-30-1.bj-qa.xxx.inc@TIGER.COM", "D:\\Work\\java\\hive.service.keytab");
        hiveConf.set("hive.metastore.uris", "thrift://dig-hadoop-30-1.bj-qa.xxxx.inc:9083,thrift://dig-hadoop-30-3.bj-qa.xxx.inc:9083");
        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
        Table table = client.getTable("default", "user_c");
        List<FieldSchema> cols = table.getSd().getCols();
        System.out.println(cols);
        client.close();
    }
}
