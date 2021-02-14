/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.memsql;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
//import com.facebook.presto.plugin.jdbc.DecimalModule;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
//import com.facebook.presto.plugin.jdbc.ForBaseJdbc;
import com.facebook.presto.plugin.jdbc.JdbcClient;
//import com.facebook.presto.plugin.jdbc.credential.CredentialProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
//import org.mariadb.jdbc.Driver;
import com.mysql.jdbc.Driver;

import java.sql.SQLException;
import java.util.Properties;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Preconditions.checkArgument;

public class MemSqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(MemSqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MemSqlConfig.class);
    }
}
