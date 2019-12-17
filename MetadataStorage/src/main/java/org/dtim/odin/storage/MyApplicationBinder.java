package org.dtim.odin.storage;

import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.jena.query.SelectQuery;
import org.dtim.odin.storage.db.mongo.repositories.*;
import org.dtim.odin.storage.service.DataSourceService;
import org.dtim.odin.storage.service.GlobalGraphService;
import org.dtim.odin.storage.service.LAVMappingService;
import org.dtim.odin.storage.service.WrapperService;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

//For injection annotation @Inject
public class MyApplicationBinder extends AbstractBinder {
    @Override
    protected void configure() {
        bind(UserRepository.class).to(UserRepository.class);
        bind(GlobalGraphRepository.class).to(GlobalGraphRepository.class);
        bind(WrapperRepository.class).to(WrapperRepository.class);
        bind(DataSourceRepository.class).to(DataSourceRepository.class);
        bind(LAVMappingRepository.class).to(LAVMappingRepository.class);
        bind(IntegratedDataSourcesRepository.class).to(IntegratedDataSourcesRepository.class);

        bind(SelectQuery.class).to(SelectQuery.class);
        bind(GraphOperations.class).to(GraphOperations.class);
        bind(SelectQuery.class).to(SelectQuery.class);

        bind(WrapperService.class).to(WrapperService.class);
        bind(LAVMappingService.class).to(LAVMappingService.class);
        bind(GlobalGraphService.class).to(GlobalGraphService.class);
        bind(DataSourceService.class).to(DataSourceService.class);
    }
}
