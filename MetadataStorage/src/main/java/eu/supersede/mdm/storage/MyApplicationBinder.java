package eu.supersede.mdm.storage;

import eu.supersede.mdm.storage.db.jena.GraphOperations;
import eu.supersede.mdm.storage.db.jena.query.SelectQuery;
import eu.supersede.mdm.storage.db.mongo.repositories.*;
import eu.supersede.mdm.storage.model.metamodel.GlobalGraph;
import eu.supersede.mdm.storage.service.DataSourceService;
import eu.supersede.mdm.storage.service.GlobalGraphService;
import eu.supersede.mdm.storage.service.LAVMappingService;
import eu.supersede.mdm.storage.service.WrapperService;
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
