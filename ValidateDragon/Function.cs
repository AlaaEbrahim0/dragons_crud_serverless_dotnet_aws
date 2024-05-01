using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ValidateDragon;

public class Function
{
    
    private AmazonSimpleSystemsManagementClient ssmClient = new();
    private AmazonS3Client s3Client = new();


    public async Task<string> FunctionHandler(Dragon dragon, ILambdaContext context)
    {
        var bucketName = await getBucketNameAsync();
        var fileName = await getFileNameAsync();

        var query = await getQuery(dragon.DragonName);

        var request = new SelectObjectContentRequest
        {
            BucketName = bucketName,
            Key = fileName,
            ExpressionType = ExpressionType.SQL,
            Expression = query,
            InputSerialization = new InputSerialization()
            {
                JSON = new JSONInput()
                {
                    JsonType = JsonType.Document
                },
            },
            OutputSerialization = new OutputSerialization()
            {
                JSON = new JSONOutput()
            }
        };

        await QueryS3(request);

        return "Dragon Validated";
        
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

    private async Task QueryS3(SelectObjectContentRequest request)
    {
        var objectContent = await s3Client.SelectObjectContentAsync(request);
        var payload = objectContent.Payload;

        var dragonData = string.Empty;
        using (payload)
        {
            foreach (var ev in payload)
            {
                if (ev is RecordsEvent records)
                {
                    using (var reader = new StreamReader(records.Payload))
                    {
                        if (reader.Peek() >= 0)
                        {
                            throw new DragonValidationException("Duplicate Dragon Reported");
                        }
                    }
                }
            }
        }
    }


    private async Task<string> getQuery(string dragonName)
    {
        return
            $"select * from s3object[*][*] s " +
            $"where s.dragon_name_str = '{dragonName}'";
    }
}
