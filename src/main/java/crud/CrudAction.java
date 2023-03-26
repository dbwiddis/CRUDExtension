package crud;


import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rest.RestStatus.OK;

import java.util.List;
import java.util.function.Function;

import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.RouteHandler;

public class CrudAction extends BaseExtensionRestHandler {

    @Override
    protected List<RouteHandler> routeHandlers() {
        return List.of(
            new RouteHandler(PUT, "sample", createHandler),
            new RouteHandler(GET, "sample", readHandler),
            new RouteHandler(POST, "sample", updateHandler),
            new RouteHandler(DELETE, "sample", deleteHandler)
        );
    }

    Function<ExtensionRestRequest, ExtensionRestResponse> createHandler = (request) -> {
        return new ExtensionRestResponse(request, OK, "To be implemented");
    };

    Function<ExtensionRestRequest, ExtensionRestResponse> readHandler = (request) -> {
        return new ExtensionRestResponse(request, OK, "To be implemented");
    };

    Function<ExtensionRestRequest, ExtensionRestResponse> updateHandler = (request) -> {
        return new ExtensionRestResponse(request, OK, "To be implemented");
    };

    Function<ExtensionRestRequest, ExtensionRestResponse> deleteHandler = (request) -> {
        return new ExtensionRestResponse(request, OK, "To be implemented");
    };
}
