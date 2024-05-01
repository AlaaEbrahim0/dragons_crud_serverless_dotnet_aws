using Amazon.Lambda.Core;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Amazon.Lambda.APIGatewayEvents;
using System.Net;
using System.Text.Json;
using Amazon.XRay.Recorder.Handlers.AwsSdk;
using Amazon.XRay.Recorder.Core;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
//[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]
namespace ListDragons;

public class Function
{

    private AmazonSimpleSystemsManagementClient ssmClient;
    private AmazonS3Client s3Client;

    public Function()
    {
        AWSSDKHandler.RegisterXRayForAllServices();
        this.ssmClient = new();
        this.s3Client = new();
    }

    public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest input, ILambdaContext context)
    {
        AWSXRayRecorder.Instance.BeginSubsegment("a/b testing");
        AWSXRayRecorder.Instance.AddAnnotation("expriement", "a");
        AWSXRayRecorder.Instance.EndSubsegment();

        string bucketName = await getBucketNameAsync();
        var fileName = await getFileNameAsync();

        var query = getQuery(input.QueryStringParameters);

        var request = new SelectObjectContentRequest
        {
            BucketName = bucketName,
            Key = fileName,
            ExpressionType = ExpressionType.SQL,
            Expression = query,
            InputSerialization = new InputSerialization()
            {
                CompressionType = CompressionType.None,
                JSON = new JSONInput()
                {
                    JsonType = JsonType.Document
                }
            },
            OutputSerialization = new OutputSerialization
            {
                JSON = new JSONOutput()
            }
        };
        var data = await QueryS3(request);

        var response = new APIGatewayProxyResponse
        {
            Body = JsonSerializer.Serialize(data, new JsonSerializerOptions
            {
                WriteIndented = true
            }),
            StatusCode = (int)HttpStatusCode.OK,
            Headers = new Dictionary<string, string>()
            {
                { "Content-Type", "application/json" },
                { "Access-Control-Allow-Origin", "*" }
            },
        };

        return response;

    }

    private async Task<List<Dragon>> QueryS3(SelectObjectContentRequest request)
    {

        var objectContent = await s3Client.SelectObjectContentAsync(request);
        var payload = objectContent.Payload;

        var dragons = new List<Dragon>();
        using (payload)
        {
            foreach (var ev in payload)
            {
                if (ev is RecordsEvent records)
                {
                    using (var reader = new StreamReader(records.Payload, Encoding.UTF8))
                    {
                        while(reader.Peek() >= 0)
                        { 
                            string dragon = reader.ReadLine();
                            var deserializedDrgaon = JsonSerializer.Deserialize<Dragon>(dragon);
                            dragons.Add(deserializedDrgaon);
                        }
                    }
                }
            }
        }
        return dragons;
    }

    private async Task<string> getBucketNameAsync()
    {
        var bucketName = await ssmClient.GetParameterAsync(new GetParameterRequest()
        {
            Name = "dragon_data_bucket_name"
        });
        return bucketName.Parameter.Value;
    }
    private async Task<string> getFileNameAsync()
    {
        var bucketName = await ssmClient.GetParameterAsync(new GetParameterRequest()
        {
            Name = "dragon_data_file_name"
        });
        return bucketName.Parameter.Value;
    }

    private string getQuery(IDictionary<string, string> queryStringParameters)
    {
        var query = "select * from S3Object[*][*] s";
        var condition = string.Empty;

        if (queryStringParameters is not null && queryStringParameters.Count != 0)
        {
            if (queryStringParameters.ContainsKey("family"))
            {
                condition += $" where s.family_str = '{queryStringParameters["family"]}'";
            }
            if (queryStringParameters.ContainsKey("dragonName"))
            {
                condition += string.IsNullOrEmpty(condition) ? " where " : " or ";
                condition += $"s.dragon_name_str = '{queryStringParameters["dragonName"]}'";
            }
        }

        query += condition;
        return query;
    }
}

