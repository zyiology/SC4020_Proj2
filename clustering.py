import pickle
import numpy as np
from sklearn.cluster import KMeans
import sys
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial.distance import cosine
from soyclustering import SphericalKMeans, visualize_pairwise_distance
from scipy.sparse import csr_matrix, vstack
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score
from sklearn.metrics import homogeneity_score, completeness_score, v_measure_score
from sklearn.metrics import silhouette_score, confusion_matrix


def compare_true_labels(true_labels, predicted_labels, data):
    # Assuming you have two numpy arrays: 'true_labels' and 'cluster_labels'
    ari = adjusted_rand_score(true_labels, predicted_labels)
    nmi = normalized_mutual_info_score(true_labels, predicted_labels)
    homogeneity = homogeneity_score(true_labels, predicted_labels)
    completeness = completeness_score(true_labels, predicted_labels)
    v_measure = v_measure_score(true_labels, predicted_labels)
    # silhouette_score requires the original data, 'X', along with the cluster labels
    silhouette = silhouette_score(data, predicted_labels, metric='cosine')

    # Print or store the results
    print("Adjusted Rand Index:", ari)
    print("Normalized Mutual Information:", nmi)
    print("Homogeneity:", homogeneity)
    print("Completeness:", completeness)
    print("V-Measure:", v_measure)
    print("Silhouette Score:", silhouette)
    return


def visualize_silhouette_clusters(cluster_labels, itemset_features, metric):
    # Assuming `cluster_labels` and `itemset_features` are from your previous clustering
    num_clusters = len(set(cluster_labels))

    # Silhouette Analysis
    silhouette_avg = silhouette_score(itemset_features, cluster_labels, metric=metric)
    print("For n_clusters =", num_clusters, "The average silhouette_score is :", silhouette_avg)

    # Compute the silhouette scores for each sample
    sample_silhouette_values = silhouette_samples(itemset_features, cluster_labels, metric=metric)

    # Silhouette plot
    fig, ax1 = plt.subplots(1, 1, figsize=(10, 6))
    ax1.set_xlim([-0.1, 1])
    ax1.set_ylim([0, len(itemset_features) + (num_clusters + 1) * 10])

    y_lower = 10
    for i in range(num_clusters):
        ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

        ith_cluster_silhouette_values.sort()

        size_cluster_i = ith_cluster_silhouette_values.shape[0]
        y_upper = y_lower + size_cluster_i

        color = cm.nipy_spectral(float(i) / num_clusters)
        ax1.fill_betweenx(np.arange(y_lower, y_upper), 0, ith_cluster_silhouette_values,
                          facecolor=color, edgecolor=color, alpha=0.7)

        # Label the silhouette plots with their cluster numbers at the middle
        ax1.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

        y_lower = y_upper + 10  # 10 for the 0 samples

    ax1.set_title("The silhouette plot for the various clusters.")
    ax1.set_xlabel("The silhouette coefficient values")
    ax1.set_ylabel("Cluster label")

    # The vertical line for average silhouette score of all the values
    ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

    ax1.set_yticks([])  # Clear the yaxis labels / ticks
    ax1.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

    plt.show()
    return

# do_pca = False
#     if do_pca:
#         # 2D Visualization
#         pca = PCA(n_components=2)
#         reduced_features = pca.fit_transform(itemset_features)
#
#         plt.figure(figsize=(10, 6))
#         for i in range(num_clusters):
#             # Separate out the data based on cluster labels
#             cluster_i = reduced_features[cluster_labels == i]
#             plt.scatter(cluster_i[:, 0], cluster_i[:, 1], label=f'Cluster {i}')
#
#         plt.title("Cluster visualization in 2D using PCA")
#         plt.legend()
#         plt.show()


def cosine_kmeans(x, n_clusters):
    length = np.sqrt((x ** 2).sum(axis=1))[:, None]
    x = x / length

    kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(x)
    return kmeans.labels_


def sph_kmeans(x, n_clusters):
    spherical_kmeans = SphericalKMeans(
        n_clusters=n_clusters,
        max_iter=10,
        verbose=1,
        init='similar_cut',
        #sparsity='minimum_df',
        minimum_df_factor=0.05
    )

    labels = spherical_kmeans.fit_predict(x)
    return labels


if __name__ == "__main__":

    # np.set_printoptions(threshold=sys.maxsize)

    with open('data/itemset_features.pkl', 'rb') as f:
        my_itemset_features = pickle.load(f)
        # for feat in my_itemset_features:
        #     print(feat)
        print("no of dimensions: ", my_itemset_features[0].size)


    # if (type(my_itemset_features)==list):
    #     my_csr_matrix = vstack([csr_matrix(arr) for arr in my_itemset_features])
    #
    #     print(type(my_csr_matrix))
    #     print(isinstance(my_csr_matrix, np.matrix))


    # Number of clusters you want
    my_num_clusters = 5

    # Perform bisecting k-means
    #my_cluster_labels = bisecting_kmeans(my_itemset_features, my_num_clusters)

    itemset_matrix = csr_matrix(my_itemset_features.astype(int))
    my_cluster_labels = sph_kmeans(itemset_matrix, my_num_clusters)

    print(my_cluster_labels)

    visualize_clusters(my_cluster_labels, my_itemset_features)





#possible clustering algorithms to explore
#partitional: bisecting k means
#agglomerative

#task4: some combo of both?

#apparently can use pairwise similarity matrix as an adjacency matrix to create a graph, then solve
#the graph (as another way to cluster)

#bisecting k means where i choose which cluster to bisect based on some criteria other than biggest