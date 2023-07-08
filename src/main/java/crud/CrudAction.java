package crud;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;

public class CrudAction extends BaseExtensionRestHandler {

    private OpenSearchClient client;

    public CrudAction(ExtensionsRunner extensionsRunner) {
        this.client = extensionsRunner.getSdkClient().initializeJavaClient();
    }

    @Override
    public List<NamedRoute> routes() {
        return List.of(
            new NamedRoute.Builder().method(Method.PUT)
                .path("/sample")
                .uniqueName("extension1:sample/create")
                .handler(createHandler)
                .build(),
            new NamedRoute.Builder().method(Method.GET)
                .path("/sample/{id}")
                .uniqueName("extension1:sample/get")
                .handler(readHandler)
                .build(),
            new NamedRoute.Builder().method(Method.POST)
                .path("/sample/{id}")
                .uniqueName("extension1:sample/post")
                .handler(updateHandler)
                .build(),
            new NamedRoute.Builder().method(Method.DELETE)
                .path("/sample/{id}")
                .uniqueName("extension1:sample/delete")
                .handler(deleteHandler)
                .build()
        );
    }

    Function<RestRequest, RestResponse> createHandler = (request) -> {
        IndexResponse response;
        try {
            BooleanResponse exists = client.indices().exists(new ExistsRequest.Builder().index("crudsample").build());
            if (!exists.value()) {
                client.indices().create(new CreateIndexRequest.Builder().index("crudsample").build());
            }
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            response = client.index(new IndexRequest.Builder<Document>().index("crudsample").document(doc).build());
        } catch (OpenSearchException | IOException e) {
            return exceptionalRequest(request, e);
        }
        return createJsonResponse(request, RestStatus.OK, "_id", response.id());
    };

    Function<RestRequest, RestResponse> readHandler = (request) -> {
        return new ExtensionRestResponse(request, RestStatus.OK, "To be implemented");
    };

    Function<RestRequest, RestResponse> updateHandler = (request) -> {
        return new ExtensionRestResponse(request, RestStatus.OK, "To be implemented");
    };

    Function<RestRequest, RestResponse> deleteHandler = (request) -> {
        return new ExtensionRestResponse(request, RestStatus.OK, "To be implemented");
    };
}
