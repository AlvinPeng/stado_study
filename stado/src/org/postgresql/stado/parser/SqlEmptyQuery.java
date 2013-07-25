/**
 * 
 */
package org.postgresql.stado.parser;

import java.util.Collection;
import java.util.Collections;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * @author Alvin Peng
 *
 */
public class SqlEmptyQuery extends DepthFirstVoidArguVisitor implements
        IXDBSql,
        IExecutable {

	public SqlEmptyQuery(XDBSessionContext client) {
    }
    
	@Override
	public long getCost() {
		return LOW_COST;
	}

	@Override
	public LockSpecification<SysTable> getLockSpecs() {
		Collection<SysTable> empty = Collections.emptyList();
        return new LockSpecification<SysTable>(empty, empty);
	}

	@Override
	public boolean needCoordinatorConnection() {
		return false;
	}

	@Override
	public ExecutionResult execute(Engine engine) throws Exception {
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_EMPTY_QUERY);
	}

	@Override
	public Collection<DBNode> getNodeList() {
		return Collections.emptyList();
	}
	
    @Override
    public boolean isReadOnly() {
        return true;
    }
}
