using dotnet_etcd;
using Etcd.Configuration.Extension.ConfigurationSource;
using Etcd.Configuration.Extension.Exceptions;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client.Configuration;
using System;
using System.Net.Http;
using System.Runtime.Intrinsics.X86;

namespace Etcd.Configuration.Extension.Client
{
    /// <summary>
    /// This is used to create an Etcd Grpc Client
    /// </summary>
    internal sealed class EtcdGrpcClientFactory: IEtcdGrpcClientFactory
    {
        //Private Variables
        private readonly IEtcdConfigurationOptions _etcdConfigurationSource;

        //Constructor
        public EtcdGrpcClientFactory(IEtcdConfigurationOptions etcdConfigurationSource)
        {
            _etcdConfigurationSource = etcdConfigurationSource;
        }

        public EtcdClient GetClient()
        {
            try 
            {
                var client = new EtcdClient(
                    connectionString: _etcdConfigurationSource.Hosts,
                    port: _etcdConfigurationSource.Port,
                    serverName: _etcdConfigurationSource.ServerName, 
                    configureChannelOptions: (options) =>
                    {
                        SocketsHttpHandler? handler = _etcdConfigurationSource.SocketsHttpHandlerForEtcd;
                        bool ssl = _etcdConfigurationSource.Ssl;
                        bool useLegacyRpcExceptionForCancellation = false;
                        MethodConfig grpcMethodConfig = new()
                        {
                            Names = { MethodName.Default },
                            RetryPolicy = new RetryPolicy
                            {
                                MaxAttempts = _etcdConfigurationSource.MaxAttempts,
                                InitialBackoff = TimeSpan.FromSeconds(_etcdConfigurationSource.InitialBackoffSeconds),
                                MaxBackoff = TimeSpan.FromSeconds(_etcdConfigurationSource.MaxBackoffSeconds),
                                BackoffMultiplier = _etcdConfigurationSource.BackoffMultiplier,
                                RetryableStatusCodes = { StatusCode.Unavailable }
                            }
                        };
                        RetryThrottlingPolicy grpcRetryThrottlingPolicy = new()
                        {
                            MaxTokens = _etcdConfigurationSource.MaxTokens,
                            TokenRatio = _etcdConfigurationSource.TokenRatio
                        };

                        options.HttpHandler = handler;
                        options.ThrowOperationCanceledOnCancellation = !useLegacyRpcExceptionForCancellation;
                        options.ServiceConfig = new ServiceConfig
                        {
                            MethodConfigs = { grpcMethodConfig },
                            RetryThrottling = grpcRetryThrottlingPolicy,
                            LoadBalancingConfigs = { new RoundRobinConfig() },
                        };

                        if (ssl)
                        {
                            options.Credentials = new SslCredentials();
                        }
                        else
                        {
#if NETCOREAPP3_1 || NETCOREAPP3_0
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
#endif
                            options.Credentials = ChannelCredentials.Insecure;
                        }

                    }, interceptors: null);
                return client;
            }
            catch(Exception ex)
            {
                var exception = new EtcdGrpcClientException("Unable to Create ETCD Grpc Client, check Inner exception to get more details", ex);
                _etcdConfigurationSource.OnClientCreationFailure?.Invoke(exception);
#pragma warning disable CS8603 // Possible null reference return.
                return null;
#pragma warning restore CS8603 // Possible null reference return.
            }
        }
    }
}
