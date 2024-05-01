using System;
using System.Text.Json.Serialization;
using Newtonsoft.Json;

public class Dragon
{
    [JsonPropertyName("description_str")]
    public string Description { get; set; }

    [JsonPropertyName("dragon_name_str")]
    public string DragonName { get; set; }

    [JsonPropertyName("family_str")]
    public string Family { get; set; }

    [JsonPropertyName("location_city_str")]
    public string LocationCity { get; set; }

    [JsonPropertyName("location_neighborhood_str")]
    public string LocationNeighborhood { get; set; }

    [JsonPropertyName("location_state_str")]
    public string LocationStateStr { get; set; }
}
