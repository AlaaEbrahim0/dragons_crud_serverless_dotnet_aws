using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AddDragon;

public class Function
{

    private AmazonSimpleSystemsManagementClient ssmClient = new();
    private AmazonS3Client s3Client = new();

    public async Task<string> FunctionHandler(Dragon dragon, ILambdaContext context)
    {
        var bucketName = await getBucketName();
        var fileName = await getFileName();

        List<Dragon> dragons;

        var s3GetResponse = await s3Client.GetObjectAsync(new()
        {
            BucketName = bucketName,
            Key = fileName,
        });

        var responseStream = s3GetResponse.ResponseStream;
        using (responseStream)
        {
            dragons = JsonSerializer.Deserialize<List<Dragon>>(responseStream);   
        }

        dragons.Add(dragon);

        var s3PutResponse = await s3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest()
        {
            BucketName = bucketName,
            Key = fileName,
            ContentBody = JsonSerializer.Serialize(dragons, new JsonSerializerOptions
            {
                WriteIndented = true
            })
        });

        return "Dragon has been added successfully";


    }

    private async Task<string> getBucketName()
    {
        var bucket = await ssmClient.GetParameterAsync(new GetParameterRequest
        {
            Name = "dragon_data_bucket_name"
        });
        return bucket.Parameter.Value;
    }
    private async Task<string> getFileName()
    {
        var file = await ssmClient.GetParameterAsync(new GetParameterRequest
        {
            Name = "dragon_data_file_name"
        });
        return file.Parameter.Value;
    }

}
