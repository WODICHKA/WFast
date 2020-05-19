using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class ActorAlreadyWork : Exception
    {
        public ActorAlreadyWork(int actorNo) : base ($"Actor already work [{actorNo}]") 
        { }

    }
}
