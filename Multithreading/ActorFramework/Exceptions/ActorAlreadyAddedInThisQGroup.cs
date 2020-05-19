using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class ActorAlreadyAddedInThisQGroup : Exception
    {

        public ActorAlreadyAddedInThisQGroup(int actorNo) : base($"Actor {actorNo} already added in this HandlersGroup")
        {

        }

    }
}
