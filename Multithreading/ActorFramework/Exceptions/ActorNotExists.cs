using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class ActorNotExists : Exception
    {
        public ActorNotExists(int actorNo) : base($"Actor [{actorNo}] not exists")
        { }
    }
}
