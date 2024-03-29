/*
 * Copyright (C) 2008-2013, fluid Operations AG
 *
 * FedX is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.fluidops.fedx.monitoring;

import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;

public abstract class Monitoring
{
	private long startTimestamp = 0;

	public abstract void monitorRemoteRequest(Endpoint e);	
	
	public abstract void resetMonitoringInformation();

	public abstract void monitorQuery(QueryInfo query);
	
	public abstract void logQueryPlan(TupleExpr tupleExpr);

	public long getStartTimestamp() {
		return this.startTimestamp;
	}

	public void monitorStartExecution() {
		this.startTimestamp = System.currentTimeMillis();
	}
}
