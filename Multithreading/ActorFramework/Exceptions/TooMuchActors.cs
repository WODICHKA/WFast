using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class TooMuchActors : Exception
    {
        public TooMuchActors(int current, int max) : base ($"Too much actors [current={current}, max={max}]") { }

    }
}
