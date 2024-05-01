using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ValidateDragon;

[Serializable]
public class DragonValidationException: Exception
{
    public DragonValidationException(string message): base(message)
    {
        
    }
}
